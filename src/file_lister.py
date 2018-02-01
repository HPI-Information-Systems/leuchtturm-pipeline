"""This job collects and dumps text documents to a spark rdd."""

import sys
import os
if os.path.exists('./libs.zip'):
    sys.path.insert(0, './libs.zip')
from settings import path_files_listed, path_emails_raw, cluster_parallelization
import json
from pyspark import SparkContext


def collect_files():
    """Read all txt documents from a folder and collect them in one rdd.

    Arguments: none.
    Returns: void.
    """
    def filter_emails(data):
        return data[1].startswith('Subject: ') or data[1].startswith('Message-ID: ')

    def create_document(data):
        return json.dumps({'doc_id': data[0].split('/')[-1].split('.')[0],
                           'path': data[0],
                           'raw': data[1]})

    sc = SparkContext()

    rdd = sc.wholeTextFiles(path_emails_raw, minPartitions=cluster_parallelization)

    rdd.filter(lambda x: filter_emails(x)) \
       .map(lambda x: create_document(x)) \
       .saveAsTextFile(path_files_listed)

    sc.stop()


if __name__ == '__main__':
    collect_files()
