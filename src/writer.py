"""Exit points for leuchtturm pipelines."""

import ujson as json

from py2neo import Graph
from pysolr import Solr


def solr_writer(rdd, solr_url='http://localhost:8983/solr/emails'):
    """Write all documents to a solr instance.

    Collect all documents of a spark rdd.
    Eliminate nested structures and upload data to solr instance.
    """
    def run_on_partition(partition):
        """Collect docs partitionswise, flatten nested structures and upload."""
        def flatten_document(dd, separator='.', prefix=''):
            """Flatten a nested pyton dict. Keys will be separated by dots."""
            return {prefix + separator + k if prefix else k: v
                    for kk, vv in dd.items()
                    for k, v in flatten_document(vv, separator, kk).items()} if isinstance(dd, dict) else {prefix: dd}

        docs_flattened = [flatten_document(json.loads(doc)) for doc in partition]
        Solr(solr_url).add(docs_flattened)

        return partition

    rdd.foreachPartition(lambda x: run_on_partition(x))


def neo4j_writer(rdd, neo4j_host='localhost', http_port=7474, bolt_port=7687):
    """Write a limited set of information contained in the email documents to a Neo4j instance.

    Collect all documents of a spark rdd.
    Extract relevant information (such as communication data) and upload it.
    """
    def run_on_partition(partition):
        """Collect docs partitionwise and upload them."""
        session = Graph(host=neo4j_host, http_port=http_port, bolt_port=bolt_port)
        for document in partition:
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

    rdd.foreachPartition(lambda x: run_on_partition(x))


def textfile_writer(rdd, path='./pipeline_result'):
    """Dump a rdd to disk as readable textfile.

    Use spark saveastextfile method to save a rdd to disk.
    Given path will be produced and must not exist. Each line will represent a document.
    """
    rdd.saveAsTextFile(path)


def arango_writer(rdd, url='http://localhost'):
    """Write all documents to arango db instance."""
    raise NotImplementedError
