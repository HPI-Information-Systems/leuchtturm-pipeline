"""This job collects and dumps text documents to a spark rdd."""

from settings import PATH_FILES_LISTED, PATH_EMAILS_RAW, CLUSTER_PARALLELIZATION
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

    rdd = sc.wholeTextFiles(PATH_EMAILS_RAW, minPartitions=CLUSTER_PARALLELIZATION)

    rdd.filter(lambda x: filter_emails(x)) \
       .map(lambda x: create_document(x)) \
       .saveAsTextFile(PATH_FILES_LISTED)

    sc.stop()


if __name__ == '__main__':
    collect_files()
