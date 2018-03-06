"""This module writes pipeline results to a solr database."""

from settings import SOLR_CLIENT_URL, PATH_PIPELINE_RESULTS
import json
import sys
import argparse
from pyspark import SparkContext
from pysolr import Solr


def write_to_solr(input_path=PATH_PIPELINE_RESULTS, solr=SOLR_CLIENT_URL):
    """Write pipeline results to a predefined solr collection.

    Requires: Text mining pipline ran.
    Arguments: none.
    Returns: void.
    """
    solr_client = Solr(solr)

    def flatten_document(dd, separator='.', prefix=''):
        return {prefix + separator + k if prefix else k: v
                for kk, vv in dd.items()
                for k, v in flatten_document(vv, separator, kk).items()} if isinstance(dd, dict) else {prefix: dd}

    sc = SparkContext()

    for part in sc.wholeTextFiles(input_path).map(lambda x: x[0]).collect():
        results = sc.textFile(part).collect()
        results = map(lambda x: flatten_document(json.loads(x)), results)

        solr_client.add(results)

    sc.stop()


if __name__ == '__main__':
    write_to_solr(input_path=sys.argv[1], solr=sys.argv[2])
