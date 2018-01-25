"""This job collects and dumps text documents to a spark rdd."""

from settings import file_lister_path_spark, pipeline_result_path_spark, cluster_parallelization
import json
from pyspark import SparkContext


def collect_files():
    """Read all txt documents from a folder and collect them in one rdd.

    Arguments: none.
    Returns: void.
    """
    def filter_emails(data):
        return data[1].startswith('Subject: ')

    def create_document(data):
        return json.dumps({'doc_id': data[0].replace('.txt', ''),
                           'raw': data[1]},
                          ensure_ascii=False)

    sc = SparkContext()

    rdd = sc.wholeTextFiles(file_lister_path_spark,
                            minPartitions=cluster_parallelization,
                            use_unicode=True)
    rdd.filter(lambda x: filter_emails(x)) \
       .map(lambda x: create_document(x)) \
       .saveAsTextFile(pipeline_result_path_spark)

    sc.stop()


if __name__ == '__main__':
    collect_files()
