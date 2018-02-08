"""This module writes pipeline results to a solr database."""

from settings import SOLR_CLIENT_URL, HDFS_CLIENT_URL, PATH_PIPELINE_RESULTS_SHORT
import pysolr
from hdfs import Client
import json


def write_to_solr():
    """Write pipeline results to a predefined solr collection.

    Requires: Text mining pipline ran.
    Arguments: none.
    Returns: void.
    """
    hdfs_client = Client(HDFS_CLIENT_URL)
    solr_client = pysolr.Solr(SOLR_CLIENT_URL)

    def flatten_document(dd, separator='.', prefix=''):
        return {prefix + separator + k if prefix else k: v
                for kk, vv in dd.items()
                for k, v in flatten_document(vv, separator, kk).items()} if isinstance(dd, dict) else {prefix: dd}

    for partition in hdfs_client.list(PATH_PIPELINE_RESULTS_SHORT):
        with hdfs_client.read(PATH_PIPELINE_RESULTS_SHORT + '/' + partition,
                              encoding='utf-8',
                              delimiter='\n') as reader:
            docs_to_push = []
            for document in reader:
                if (len(document) != 0):
                    docs_to_push.append(flatten_document(json.loads(document)))
                if (len(docs_to_push) % 1000 == 0):
                    solr_client.add(docs_to_push)
                    docs_to_push = []
            if (len(docs_to_push)):
                solr_client.add(docs_to_push)
                docs_to_push = []


if __name__ == '__main__':
    write_to_solr()
