"""Exit points for leuchtturm pipelines."""

import json

from neo4j.v1 import DirectDriver
from pysolr import Solr

from .common import Pipe


class SolrWriter(Pipe):
    """Write all documents to a solr instance.

    Collect all documents of a spark rdd.
    Eliminate nested structures and upload data to solr instance.
    """

    def __init__(self, solr_url='http://localhost:8983/solr/emails'):
        """Set url to solr instance."""
        super().__init__()
        self.solr_url = solr_url
        self.solr_client = Solr(self.solr_url)

    def flatten_document(self, dd, separator='.', prefix=''):
        """Flatten a nested pyton dict. Keys will be separated by dots."""
        return {prefix + separator + k if prefix else k: v
                for kk, vv in dd.items()
                for k, v in self.flatten_document(vv, separator, kk).items()} if isinstance(dd, dict) else {prefix: dd}

    def run_on_partition(self, partition):
        """Collect docs partitionswise, flatten nested structures and upload."""
        docs = partition.collect()
        docs_flattened = [self.flatten_document(json.loads(doc)) for doc in docs]
        self.solr_client.add(docs_flattened)

    def run(self, rdd):
        """Run task in spark context."""
        rdd.mapPartitions(lambda x: self.run_on_partition(x))


class Neo4JWriter(Pipe):
    """Write a limited set of information contained in the email documents to a Neo4j instance.

    Collect all documents of a spark rdd.
    Extract relevant information (such as communication data) and upload it.
    """

    def __init__(self, neo4j_uri='bolt://localhost:7687'):
        """Set Neo4j instance where data sould be uploaded to."""
        super().__init__()
        self.neo4j_uri = neo4j_uri
        self.neo4j_client = DirectDriver(self.neo4j_uri)

    def run_on_partition(self, partition):
        """Collect docs partitionwise and upload them."""
        with self.neo4j_client.session() as session:
            for document in partition.collect():
                if (len(document) != 0):
                    sender = {"name": "", "email": ""}
                    recipients = []
                    mail_id = ""
                    mail = json.loads(document)
                    if 'sender' in mail['header'].keys():
                        sender = mail['header']['sender']
                    if 'recipients' in mail['header'].keys():
                        recipients = mail['header']['recipients']
                    if 'doc_id' in mail.keys():
                        mail_id = mail['doc_id']

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
                                    "ON CREATE SET w.mail_list = [$mail_id] "
                                    "ON MATCH SET w.mail_list = "
                                    "CASE WHEN NOT $mail_id IN w.mail_list "
                                    "THEN w.mail_list + $mail_id "
                                    "ELSE w.mail_list END",
                                    name_sender=sender['name'],
                                    email_sender=sender['email'],
                                    name_recipient=recipient['name'],
                                    email_recipient=recipient['email'],
                                    mail_id=mail_id)

    def run(self, rdd):
        """Run task in spark context."""
        rdd.mapPartitions(lambda x: self.run_on_partition(x))


class TextfileWriter(Pipe):
    """Dump a rdd to disk as readable textfile.

    Use spark saveastextfile method to save a rdd to disk.
    Given path will be produced and must not exist. Each line will represent a document.
    """

    def __init__(self, path='./pipeline'):
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
