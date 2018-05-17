"""Exit points for leuchtturm pipelines."""

import ujson as json
from datetime import datetime
import time

from py2neo import Graph
from pysolr import Solr
from .common import Pipe, SparkProvider


class SolrWriter(Pipe):
    """Write all documents to a solr instance.

    Collect all documents of a spark rdd.
    Eliminate nested structures and upload data to solr instance.
    NOTE: Does not run on large or highly distributed rdds. Use SolrFileWriter instead.
    """

    def __init__(self, conf, solr_url):
        """Set url of solr instance."""
        super().__init__(conf)
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

    def __init__(self, conf, path, solr_url):
        """Set solr config and path where rdd is read from."""
        super().__init__(conf)
        self.conf = conf
        self.path = path
        self.solr_url = solr_url
        self.solr_writer = SolrWriter(conf, self.solr_url)

    def run(self):
        """Run task in spark context."""
        sc = SparkProvider.spark_context(self.conf)
        for part in sc.wholeTextFiles(self.path).map(lambda x: x[0]).collect():
            results = sc.textFile(part)
            self.solr_writer.run(results)


class Neo4JNodeWriter(Pipe):
    """Write a limited set of correspondent information to a Neo4j instance.

    Collect all correspondent information from a spark rdd and upload it.
    NOTE: Does not run on large or highly distributed rdds. Use Neo4JFileWriter instead.
    """

    def __init__(self, conf):
        """Set Neo4j instance config."""
        super().__init__(conf)
        self.neo4j_host = conf.get('neo4j', 'host')
        self.http_port = conf.get('neo4j', 'http_port')
        self.bolt_port = conf.get('neo4j', 'bolt_port')

    def run_on_partition(self, partition):
        """Collect docs partitionwise and upload them."""
        start_time = datetime.now()
        print('lt_logs', start_time, 'Start Neo4j Node Upload on partition...', flush=True)
        correspondents = [json.loads(item) for item in partition]
        graph = Graph(host=self.neo4j_host, http_port=self.http_port, bolt_port=self.bolt_port)
        graph.run('UNWIND $correspondents AS correspondent '
                  'CREATE (a:Person) SET a = correspondent',
                  correspondents=correspondents)
        print('lt_logs', datetime.now(), 'Finish Neo4j Node Upload on partition from', start_time, flush=True)

    def run(self, rdd):
        """Run task in spark context."""
        rdd.coalesce(1) \
           .foreachPartition(self.run_on_partition)


class Neo4JEdgeWriter(Pipe):
    """Write a limited set of information contained in the email documents to a Neo4j instance.

    Collect all documents of a spark rdd.
    Extract relevant information (such as communication data) and upload it.
    NOTE: Does not run on large or highly distributed rdds. Use Neo4JFileWriter instead.
    """

    def __init__(self, conf):
        """Set Neo4j instance config."""
        super().__init__(conf)
        self.neo4j_host = conf.get('neo4j', 'host')
        self.http_port = conf.get('neo4j', 'http_port')
        self.bolt_port = conf.get('neo4j', 'bolt_port')

    def run_on_partition(self, partition):
        """Collect docs partitionwise and upload them."""
        start_time = datetime.now()
        print('lt_logs', start_time, 'Start Neo4j Edge Upload on partition...', flush=True)
        documents = [json.loads(item) for item in partition]
        graph = Graph(host=self.neo4j_host, http_port=self.http_port, bolt_port=self.bolt_port)
        for mail in documents:
            mail_id = mail.get('doc_id', '')
            header = mail.get('header', {})
            sender = header.get('sender', {'identifying_name': '', 'name': '', 'email': ''})
            recipients = header.get('recipients', [{'identifying_name': ''}])
            try:
                mail_timestamp = time.mktime(
                    datetime.strptime(header.get('date', ''), "%Y-%m-%dT%H:%M:%SZ").timetuple()
                )
            except Exception:
                mail_timestamp = 0.0  # timestamp for 1970-01-01T00:00:00+00:00'

            step_size = 10
            for i in range(0, len(recipients), step_size):
                edge_start_time = datetime.now()
                print('lt_logs', edge_start_time, 'Upload edge', mail.get('path'), 'with', recipients[i:i + step_size])
                graph.run(
                    'MATCH '
                        '(a:Person {identifying_name: $sender_identifying_name}) '
                    'UNWIND $recipients AS recipient '
                    'MATCH '
                        '(b:Person {identifying_name: recipient.identifying_name}) '
                    'MERGE (a)-[w:WRITESTO]->(b) '
                        'ON CREATE SET '
                            'w.mail_list = [$mail_id], '
                            'w.time_list = [$mail_timestamp] '
                        'ON MATCH SET '
                            'w.mail_list = w.mail_list + $mail_id, '
                            'w.time_list = w.time_list + $mail_timestamp',
                    recipients=recipients[i:i + step_size],
                    sender_identifying_name=sender['identifying_name'],
                    mail_id=mail_id,
                    mail_timestamp=mail_timestamp
                )  # noqa
                print('lt_logs', datetime.now(), 'Finish uploading edge from', edge_start_time)
        print('lt_logs', datetime.now(), 'Finish Neo4j Edge Upload on partition from', start_time, flush=True)

    def run(self, rdd):
        """Run task in spark context."""
        rdd.coalesce(1) \
           .foreachPartition(self.run_on_partition)


class Neo4JFileWriter(Pipe):
    """Allow upload of large rdds to neo4j.

    Less performant than Neo4JWriter, but Neo4J doesn't crash for large uploads.
    Utilizes Neo4JNodeWriter for nodes or Neo4jEdgeWriter for edgesunder the hood.
    """

    def __init__(self, conf, path, mode):
        """Set Neo4j instance config."""
        super().__init__(conf)
        self.conf = conf
        self.path = path
        self.mode = mode
        if self.mode == 'nodes':
            self.neo4j_writer = Neo4JNodeWriter(conf)
        elif self.mode == 'edges':
            self.neo4j_writer = Neo4JEdgeWriter(conf)
        else:
            raise Exception

    def run(self):
        """Run task in spark context."""
        sc = SparkProvider.spark_context(self.conf)
        for part in sc.wholeTextFiles(self.path).map(lambda x: x[0]).collect():
            results = sc.textFile(part)
            self.neo4j_writer.run(results)
        if self.mode == 'nodes' and self.conf.get('neo4j', 'create_node_index'):
            graph = Graph(
                host=self.conf.get('neo4j', 'host'),
                http_port=self.conf.get('neo4j', 'http_port'),
                bolt_port=self.conf.get('neo4j', 'bolt_port')
            )
            graph.schema.create_index('Person', 'identifying_name')


class TextFileWriter(Pipe):
    """Dump a rdd to disk as readable textfile.

    Use spark saveastextfile method to save a rdd to disk.
    Given path will be produced and must not exist. Each line will represent a document.
    """

    def __init__(self, conf, path):
        """Set output path."""
        super().__init__(conf)
        self.path = path

    def run(self, rdd):
        """Run task in spark context."""
        rdd.saveAsTextFile(self.path)


class ArangoWriter(Pipe):
    """Write all documents to arango db instance."""

    def __init__(self):
        """IMPLEMENT ME."""
        raise NotImplementedError
