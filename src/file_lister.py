"""This collects and dumps text documents to a spark rdd."""

import json
from hdfs import InsecureClient
import findspark
findspark.init('/usr/hdp/2.6.3.0-235/spark2')
from pyspark import SparkContext


input_folder = 'hdfs://172.18.20.109/enron_text'
output_path = 'hdfs://172.18.20.109/pipeline/files_listed'
hdfs_client = InsecureClient('http://b7689.byod.hpi.de:50070')


"""Read all txt documents from a folder and collect them in one rdd.

Arguments: none.
Returns: void.
"""
def collect_files():
    """Run file listing."""
    def filter_emails(data):
        return data[1].startswith('Subject:')

    def create_document(data):
        return json.dumps({'doc_id': data[0].replace('.txt', ''),
                           'raw': data[1]},
                          ensure_ascii=False)

    sc = SparkContext()

    rdd = sc.emptyRDD()

    for directory in hdfs_client.list(input_folder):
        for subdirectory in hdfs_client.list(input_folder + '/' + directory):
            rdd = rdd.union(sc.wholeTextFiles(input_folder + '/' + directory + '/' + subdirectory,
                                              minPartitions=None,
                                              use_unicode=True))

    print(rdd.count())

    rdd = rdd.filter(lambda x: filter_emails(x)) \
             .map(lambda x: create_document(x))

    print(rdd.count())

    rdd.saveAsTextFile(output_path)

    sc.stop()


if __name__ == '__main__':
    collect_files()
