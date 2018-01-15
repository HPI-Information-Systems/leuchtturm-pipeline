"""This module writes pipeline results to a solr database."""

import pysolr
from hdfs import Client
from flatten_dict import flatten
import json


input_path = '/pipeline/pipeline_results'
hdfs_client = Client('http://172.18.20.109:50070')
solr_collection = pysolr.Solr('http://b1184.byod.hpi.de:8983/solr/allthemails')


def write_to_solr():
    """Write pipeline results to a predefined solr collection.

    Requires: Text mining pipline ran.
    Arguments: none.
    Returns: void.
    """
    def dot_reducer(k1, k2):
        if k1 is None:
            return k2
        else:
            return str(k1) + '.' + str(k2)

    def flatten_document(document):
        document = json.loads(document)
        document['parts'] = dict(enumerate(document['parts']))

        return json.dumps(flatten(document, reducer=dot_reducer), ensure_ascii=False)

    for partition in hdfs_client.list(input_path):
        with hdfs_client.read(input_path + '/' + partition, encoding='utf-8', delimiter='\n') as reader:
            docs_to_push = []
            count = 0
            for document in reader:
                docs_to_push.append(flatten_document(document))
                count += 1
                if (count % 300 == 0):
                    try:
                        solr_collection.add(docs_to_push)
                    except Exception:
                        print('Failure')
                    docs_to_push = []
            if (len(docs_to_push)):
                try:
                    solr_collection.add(docs_to_push)
                except Exception:
                    print('Failure')
                docs_to_push = []


if __name__ == '__main__':
    write_to_solr()
