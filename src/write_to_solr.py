"""This module writes pipeline results to a solr database."""

from settings import solr_url, hdfs_client_url, pipeline_result_path_hdfs_client
import pysolr
from hdfs import Client
from flatten_dict import flatten
import json


def write_to_solr():
    """Write pipeline results to a predefined solr collection.

    Requires: Text mining pipline ran.
    Arguments: none.
    Returns: void.
    """
    hdfs_client = Client(hdfs_client_url)
    solr_client = pysolr.Solr(solr_url)

    def dot_reducer(k1, k2):
        if k1 is None:
            return k2
        else:
            return str(k1) + '.' + str(k2)

    def flatten_document(document):
        return flatten(json.loads(document), reducer=dot_reducer)

    for partition in hdfs_client.list(pipeline_result_path_hdfs_client):
        with hdfs_client.read(pipeline_result_path_hdfs_client + '/' + partition,
                              encoding='utf-8',
                              delimiter='\n') as reader:
            docs_to_push = []
            for document in reader:
                if (len(document) != 0):
                    docs_to_push.append(flatten_document(document))
                if (len(docs_to_push) % 1000 == 0):
                    solr_client.add(docs_to_push)
                    docs_to_push = []
            if (len(docs_to_push)):
                solr_client.add(docs_to_push)
                docs_to_push = []


if __name__ == '__main__':
    write_to_solr()
