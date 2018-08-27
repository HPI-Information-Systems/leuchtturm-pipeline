"""Exit points for leuchtturm pipelines."""

import ujson as json
from datetime import datetime
import time
import shutil

from py2neo import Graph
from pysolr import Solr
from .common import Pipe, SparkProvider


class SolrUploader(Pipe):
    def __init__(self, conf, collection):
        """Set solr config and path where rdd is read from."""
        super().__init__(conf)
        self.conf = conf
        self.rows_per_request = conf.get('solr', 'rows_per_request')
        self.solr_url = conf.solr_url + collection

    def run_on_partition(self, partition):
        def flatten_document(dd, separator='.', prefix=''):
            """Flatten a nested python dict. Function in method because cannot pickle recursive methods."""
            return {prefix + separator + k if prefix else k: v
                    for kk, vv in dd.items()
                    for k, v in flatten_document(vv, separator, kk).items()} if isinstance(dd, dict) else {prefix: dd}

        solr_client = Solr(self.solr_url)
        buffer = []
        for doc_str in partition:
            doc = json.loads(doc_str)
            buffer.append(flatten_document(doc))
            if len(buffer) >= self.rows_per_request:
                solr_client.add(buffer)
                buffer = []
        solr_client.add(buffer)

    def run(self, rdd):
        rdd.coalesce(1).foreachPartition(lambda x: self.run_on_partition(x))


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
        print('lt_logs', 'ports: {} | {}'.format(self.http_port, self.bolt_port), flush=True)
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

    def prepare_for_upload(self, data):
        """Reduce complexity of an email object so that it can be easier consumed by Neo4j."""
        document = json.loads(data)

        mail_id = document.get('doc_id', '')
        header = document.get('header', {})
        sender = header.get('sender', {})
        recipients = header.get('recipients', [{}])
        path = document.get('path', '')
        try:
            mail_timestamp = time.mktime(
                datetime.strptime(header.get('date', ''), "%Y-%m-%dT%H:%M:%SZ").timetuple()
            )
        except Exception:
            mail_timestamp = 0.0  # timestamp for 1970-01-01T00:00:00+00:00'

        result_document = {
            'mail_id': mail_id,
            'sender_identifying_name': sender.get('identifying_name', ''),
            'recipient_identifying_names': [recipient.get('identifying_name', '') for recipient in recipients],
            'path': path,
            'mail_timestamp': mail_timestamp
        }

        return json.dumps(result_document)

    def run_on_partition(self, partition):
        """Collect docs partitionwise and upload them."""
        start_time = datetime.now()
        print('lt_logs', start_time, 'Start Neo4j Edge Upload on partition...', flush=True)
        documents = [json.loads(item) for item in partition]
        graph = Graph(host=self.neo4j_host, http_port=self.http_port, bolt_port=self.bolt_port)
        graph.run(
            'UNWIND $mails AS mail '
            'MATCH '
            '(a:Person {identifying_name: mail.sender_identifying_name}) '
            'UNWIND mail.recipient_identifying_names AS recipient_identifying_name '
            'MATCH '
            '(b:Person {identifying_name: recipient_identifying_name}) '
            'MERGE (a)-[w:WRITESTO]->(b) '
            'ON CREATE SET '
            'w.mail_list = [mail.mail_id], '
            'w.time_list = [mail.mail_timestamp] '
            'ON MATCH SET '
            'w.mail_list = w.mail_list + mail.mail_id, '
            'w.time_list = w.time_list + mail.mail_timestamp',
            mails=documents
        )  # noqa
        print('lt_logs', datetime.now(), 'Finish Neo4j Edge Upload on partition from', start_time, flush=True)

    def run(self, rdd):
        """Run task in spark context."""
        rdd.map(self.prepare_for_upload) \
            .coalesce(1) \
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
            print('bolt port:', self.conf.get('neo4j', 'bolt_port'))
            graph = Graph(
                host=self.conf.get('neo4j', 'host'),
                http_port=self.conf.get('neo4j', 'http_port'),
                bolt_port=self.conf.get('neo4j', 'bolt_port')
            )
            graph.schema.create_index('Person', 'identifying_name')


class Neo4JNodeUploader(Pipe):
    def __init__(self, conf):
        """Set Neo4j instance config."""
        super().__init__(conf)
        self.conf = conf
        self.neo4j_host = conf.get('neo4j', 'host')
        self.http_port = conf.get('neo4j', 'http_port')
        self.bolt_port = conf.get('neo4j', 'bolt_port')
        self.buffer_size = conf.get('neo4j', 'rows_per_request')

    def upload_batch(self, neo_connection, correspondents):
        neo_connection.run('UNWIND $correspondents AS correspondent '
                           'CREATE (a:Person) SET a = correspondent',
                           correspondents=correspondents)

    def run_on_partition(self, partition):
        neo_connection = Graph(host=self.neo4j_host, http_port=self.http_port, bolt_port=self.bolt_port)
        buffer = []
        for doc_str in partition:
            buffer.append(json.loads(doc_str))
            if len(buffer) > self.buffer_size:
                self.upload_batch(neo_connection, buffer)
                buffer = []
        self.upload_batch(neo_connection, buffer)

    def run(self, rdd):
        start_time = datetime.now()
        print('lt_logs', start_time, 'Start Neo4j Node Upload on partition...', flush=True)
        rdd.coalesce(1)\
            .foreachPartition(self.run_on_partition)
        print('lt_logs', datetime.now(), 'Finish Neo4j Node Upload on partition from', start_time, flush=True)

        if self.conf.get('neo4j', 'create_node_index'):
            print('Creating node index in neo4j for person names (on identifying_name) ...')
            self.neo_connection.schema.create_index('Person', 'identifying_name')
            print('Done with index!')


class Neo4JEdgeUploader(Pipe):
    def __init__(self, conf):
        """Set Neo4j instance config."""
        super().__init__(conf)
        self.conf = conf
        self.neo4j_host = conf.get('neo4j', 'host')
        self.http_port = conf.get('neo4j', 'http_port')
        self.bolt_port = conf.get('neo4j', 'bolt_port')
        self.buffer_size = conf.get('neo4j', 'rows_per_request')

    def prepare_for_upload(self, data):
        """Reduce complexity of an email object so that it can be easier consumed by Neo4j."""
        document = json.loads(data)

        mail_id = document.get('doc_id', '')
        header = document.get('header', {})
        sender = header.get('sender', {})
        recipients = header.get('recipients', [{}])
        path = document.get('path', '')
        try:
            mail_timestamp = time.mktime(
                datetime.strptime(header.get('date', ''), "%Y-%m-%dT%H:%M:%SZ").timetuple()
            )
        except Exception:
            mail_timestamp = 0.0  # timestamp for 1970-01-01T00:00:00+00:00'

        result_document = {
            'mail_id': mail_id,
            'sender_identifying_name': sender.get('identifying_name', ''),
            'recipient_identifying_names': [recipient.get('identifying_name', '') for recipient in recipients],
            'path': path,
            'mail_timestamp': mail_timestamp
        }

        return json.dumps(result_document)

    def upload_batch(self, neo_connection, documents):
        self.neo_connection.run(
            'UNWIND $mails AS mail '
            'MATCH '
            '(a:Person {identifying_name: mail.sender_identifying_name}) '
            'UNWIND mail.recipient_identifying_names AS recipient_identifying_name '
            'MATCH '
            '(b:Person {identifying_name: recipient_identifying_name}) '
            'MERGE (a)-[w:WRITESTO]->(b) '
            'ON CREATE SET '
            'w.mail_list = [mail.mail_id], '
            'w.time_list = [mail.mail_timestamp] '
            'ON MATCH SET '
            'w.mail_list = w.mail_list + mail.mail_id, '
            'w.time_list = w.time_list + mail.mail_timestamp',
            mails=documents
        )

    def run_on_partition(self, partition):
        neo_connection = Graph(host=self.neo4j_host, http_port=self.http_port, bolt_port=self.bolt_port)
        buffer = []
        for doc_str in partition:
            buffer.append(json.loads(doc_str))
            if len(buffer) > self.buffer_size:
                self.upload_batch(neo_connection, buffer)
                buffer = []
        self.upload_batch(neo_connection, buffer)

    def run(self, rdd):
        start_time = datetime.now()
        print('lt_logs', start_time, 'Start Neo4j Edge Upload on partition...', flush=True)
        rdd.map(self.prepare_for_upload)\
            .coalesce(1) \
            .foreachPartition(self.run_on_partition)
        print('lt_logs', datetime.now(), 'Finish Neo4j Edge Upload on partition from', start_time, flush=True)


class TextFileWriter(Pipe):
    """Dump a rdd to disk as readable textfile.

    Use spark saveastextfile method to save a rdd to disk.
    Given path will be produced and must not exist. Each line will represent a document.
    """

    def __init__(self, conf, path):
        """Set output path."""
        super().__init__(conf)
        # shutil.rmtree(path, ignore_errors=True)
        self.path = path

    def run(self, rdd):
        """Run task in spark context."""
        rdd.saveAsTextFile(self.path)


class ArangoWriter(Pipe):
    """Write all documents to arango db instance."""

    def __init__(self):
        """IMPLEMENT ME."""
        raise NotImplementedError
