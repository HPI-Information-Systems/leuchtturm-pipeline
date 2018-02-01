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

    def flatten_document(document):
        def flatten_dict(current, key='', result={}):
            if isinstance(current, type(dict)):
                for k in current:
                    new_key = "{0}.{1}".format(key, k) if len(key) > 0 else k
                    flatten_dict(current[k], new_key, result)
                else:
                    result[key] = current
            return result

        return flatten_dict(json.loads(document))

    for partition in hdfs_client.list(PATH_PIPELINE_RESULTS_SHORT):
        with hdfs_client.read(PATH_PIPELINE_RESULTS_SHORT + '/' + partition,
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
