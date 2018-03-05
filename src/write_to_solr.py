"""This module writes pipeline results to a solr database."""

from settings import SOLR_CLIENT_URL, PATH_PIPELINE_RESULTS
import json
import os
from pyspark import SparkContext
from pysolr import Solr


def write_to_solr():
    """Write pipeline results to a predefined solr collection.

    Requires: Text mining pipline ran.
    Arguments: none.
    Returns: void.
    """
    solr_client = Solr(SOLR_CLIENT_URL)

    def flatten_document(dd, separator='.', prefix=''):
        return {prefix + separator + k if prefix else k: v
                for kk, vv in dd.items()
                for k, v in flatten_document(vv, separator, kk).items()} if isinstance(dd, dict) else {prefix: dd}

    sc = SparkContext()

    command = 'hadoop fs -ls {} | sed "1d;s/  */ /g" | cut -d\  -f8'.format(PATH_PIPELINE_RESULTS)
    for part in os.popen(command).read().splitlines():
        results = sc.textFile(part).collect()
        results = map(lambda x: flatten_document(json.loads(x)), results)

        solr_client.add(results)

    sc.stop()


if __name__ == '__main__':
    write_to_solr()
