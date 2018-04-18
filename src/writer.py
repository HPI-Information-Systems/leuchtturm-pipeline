"""Exit points for leuchtturm pipelines."""

import ujson as json

from py2neo import Graph
from pysolr import Solr

from .common import Pipe, SparkProvider


class SolrWriter(Pipe):
    """Write all documents to a solr instance.

    Collect all documents of a spark rdd.
    Eliminate nested structures and upload data to solr instance.
    NOTE: Does not run on large or highly distributed rdds. Use SolrFileWriter instead.
    """

    def __init__(self, solr_url='http://sopedu.hpi.uni-potsdam.de:8983/solr/emails'):
        """Set url of solr instance."""
        super().__init__()
        self.solr_url = solr_url

    def run_on_partition(self, partition):
        """Collect docs partitionswise, flatten nested structures and upload."""
        def flatten_document(dd, separator='.', prefix=''):
            """Flatten a nested python dict. Function in method because cannot pickle recursive methods."""
            return {prefix + separator + k if prefix else k: v
                    for kk, vv in dd.items()
                    for k, v in flatten_document(vv, separator, kk).items()} if isinstance(dd, dict) else {prefix: dd}

        docs_flattened = [flatten_document(json.loads(doc)) for doc in partition]
        Solr(self.solr_url).add(docs_flattened)

    def run(self, rdd):
        """Run task in spark context."""
        rdd.coalesce(1) \
           .foreachPartition(lambda x: self.run_on_partition(x))


class SolrFileWriter(Pipe):
    """Allow upload of large rdds to solr.

    Less performant than SolrWriter, but Solr doesn't crash for large uploads.
    Utilizes SolrWriter under the hood.
    """

    def __init__(self, path, solr_url='http://sopedu.hpi.uni-potsdam.de:8983/solr/emails'):
        """Set solr config and path where rdd is read from."""
        super().__init__()
        self.path = path
        self.solr_url = solr_url
        self.solr_writer = SolrWriter(solr_url=self.solr_url)

    def run(self):
        """Run task in spark context."""
        sc = SparkProvider.spark_context()
        for part in sc.wholeTextFiles(self.path).map(lambda x: x[0]).collect():
            results = sc.textFile(part)
            self.solr_writer.run(results)


class Neo4JWriter(Pipe):
    """Write a limited set of information contained in the email documents to a Neo4j instance.

    Collect all documents of a spark rdd.
    Extract relevant information (such as communication data) and upload it.
    NOTE: Does not run on large or highly distributed rdds. Use Neo4JFileWriter instead.
    """

    def __init__(self, neo4j_host='sopedu.hpi.uni-potsdam.de', http_port=7474, bolt_port=7687):
        """Set Neo4j instance config."""
        super().__init__()
        self.neo4j_host = neo4j_host
        self.http_port = http_port
        self.bolt_port = bolt_port

    def run_on_partition(self, partition):
        """Collect docs partitionwise and upload them."""
        session = Graph(self.neo4j_host, http_port=self.http_port, bolt_port=self.bolt_port)
        for document in partition:
            if (len(document) != 0):
                sender = {"name": "", "email": ""}
                recipients = []
                mail_id = ""
                mail_timestamp = ""
                mail = json.loads(document)
                if 'sender' in mail['header'].keys():
                    sender = mail['header']['sender']
                if 'recipients' in mail['header'].keys():
                    recipients = mail['header']['recipients']
                if 'doc_id' in mail.keys():
                    mail_id = mail['doc_id']
                if 'header' in mail.keys():
                    if 'date' in mail['header']:
                        mail_timestamp = mail['header']['date']

                for recipient in recipients:
                    session.run("MERGE (sender:Person {email: $email_sender}) "
                                "ON CREATE SET sender.name = [$name_sender] "
                                "ON MATCH SET sender.name = "
                                "CASE WHEN NOT $name_sender IN sender.name "
                                "THEN sender.name + $name_sender "
                                "ELSE sender.name END "
                                "MERGE (recipient:Person {email: $email_recipient}) "
                                "ON CREATE SET recipient.name = [$name_recipient] "
                                "ON MATCH SET recipient.name = "
                                "CASE WHEN NOT $name_recipient IN recipient.name "
                                "THEN recipient.name + $name_recipient "
                                "ELSE recipient.name END "
                                "MERGE (sender)-[w:WRITESTO]->(recipient) "
                                "ON CREATE SET w.mail_list = [$mail_id], w.time_list = [$mail_timestamp] "
                                "ON MATCH SET w.mail_list = "
                                "CASE WHEN NOT $mail_id IN w.mail_list "
                                "THEN w.mail_list + $mail_id "
                                "ELSE w.mail_list END, "
                                "w.time_list = "
                                "CASE WHEN NOT $mail_timestamp IN w.time_list "
                                "THEN w.time_list + $mail_timestamp "
                                "ELSE w.time_list END",
                                name_sender=sender['name'],
                                email_sender=sender['email'],
                                name_recipient=recipient['name'],
                                email_recipient=recipient['email'],
                                mail_id=mail_id,
                                mail_timestamp=mail_timestamp)

    def run(self, rdd):
        """Run task in spark context."""
        rdd.coalesce(1) \
           .foreachPartition(lambda x: self.run_on_partition(x))


class Neo4JFileWriter(Pipe):
    """Allow upload of large rdds to neo4j.

    Less performant than Neo4JWriter, but Neo4J doesn't crash for large uploads.
    Utilizes Neo4JWriter under the hood.
    """

    def __init__(self, neo4j_host='sopedu.hpi.uni-potsdam.de', http_port=7474, bolt_port=7687):
        """Set Neo4j instance config."""
        super().__init__()
        self.neo4j_host = neo4j_host
        self.http_port = http_port
        self.bolt_port = bolt_port
        self.neo4j_writer = Neo4JWriter(neo4j_host=self.neo4j_host, http_port=self.http_port, bolt_port=self.bolt_port)

    def run(self):
        """Run task in spark context."""
        sc = SparkProvider.spark_context()
        for part in sc.wholeTextFiles(self.path).map(lambda x: x[0]).collect():
            results = sc.textFile(part)
            self.neo4j_writer.run(results)


class TextFileWriter(Pipe):
    """Dump a rdd to disk as readable textfile.

    Use spark saveastextfile method to save a rdd to disk.
    Given path will be produced and must not exist. Each line will represent a document.
    """

    def __init__(self, path='./tmp'):
        """Set output path."""
        super().__init__()
        self.path = path

    def run(self, rdd):
        """Run task in spark context."""
        rdd.saveAsTextFile(self.path)


class ArangoWriter(Pipe):
    """Write all documents to arango db instance."""

    def __init__(self):
        """IMPLEMENT ME."""
        raise NotImplementedError
