"""This module writes pipeline results to a solr database."""

import pysolr
from flatten_dict import flatten
import json
import findspark
findspark.init('/usr/hdp/2.6.3.0-235/spark2')
from pyspark import SparkContext


input_path = "/path/to/files/listed/data-frame"
solr_collection = pysolr.Solr('http://b1184.byod.hpi.de:8983/solr/allthemails')


"""Write pipeline results to a predefined solr collection.

Requires: Text mining pipline ran.
Arguments: none.
Returns: void.
"""
def write_to_solr():
    """Iterate over documents and add them to solr db."""
    def dot_reducer(k1, k2):
        if k1 is None:
            return k2
        else:
            return str(k1) + "." + str(k2)

    def flatten_document(document):
        document = json.loads(document)
        document["parts"] = dict(enumerate(document["parts"]))

        return json.dumps(flatten(document, reducer=dot_reducer))

    # TODO: add config, or maybe not for a db task?!
    sc = SparkContext()

    documents = sc.textFile(input_path)

    step = 100
    for idx in range(0, documents.size(), step):
        docs_to_push = documents.range(idx, idx + step).map(lambda x: flatten_document(x)).collect()
        try:
            solr_collection.add(docs_to_push)
        except Exception:
            print('==============> Failure on chunk : ' + idx + ' <==============')

    sc.stop()


if __name__ == "__main__":
    write_to_solr()
