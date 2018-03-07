"""This job collects and dumps text documents to a spark rdd."""

from settings import PATH_FILES_LISTED, PATH_EMAILS_RAW
import json
import email
import sys
from pyspark import SparkContext, SparkConf


def collect_files(input_path=PATH_EMAILS_RAW, output_path=PATH_FILES_LISTED):
    """Read all txt documents from a folder and collect them in one rdd.

    Arguments: none.
    Returns: void.
    """
    def filter_emails(data):
        return len(email.message_from_string(data[1]).defects) == 0

    def create_document(data):
        return json.dumps({'doc_id': data[0].split('/')[-1].split('.')[0],
                           'path': data[0],
                           'raw': data[1]})

    config = SparkConf().set('spark.hive.mapred.supports.subdirectories', 'true') \
                        .set('spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive', 'true') \
                        .set('spark.default.parallelism', 276)

    sc = SparkContext(conf=config)
    sc.setLogLevel('WARN')

    rdd = sc.wholeTextFiles(input_path, 276)

    rdd.filter(lambda x: filter_emails(x)) \
       .map(lambda x: create_document(x)) \
       .saveAsTextFile(output_path)

    sc.stop()


if __name__ == '__main__':
    collect_files(input_path=sys.argv[1], output_path=sys.argv[2])
