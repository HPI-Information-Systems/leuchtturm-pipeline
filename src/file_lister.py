"""This job collects and dumps text documents to a spark rdd."""

import json
from pyspark import SparkContext


input_path = 'hdfs://172.18.20.109/enron_text/*/*/*'
output_path = 'hdfs://172.18.20.109/pipeline/files_listed'


def collect_files():
    """Read all txt documents from a folder and collect them in one rdd.

    Arguments: none.
    Returns: void.
    """
    def filter_emails(data):
        return data[1].startswith('Subject:')

    def create_document(data):
        return json.dumps({'doc_id': data[0].replace('.txt', ''),
                           'raw': data[1]},
                          ensure_ascii=False)

    sc = SparkContext()

    rdd = sc.wholeTextFiles(input_path,
                            minPartitions=24,
                            use_unicode=True)
    rdd.filter(lambda x: filter_emails(x)) \
       .map(lambda x: create_document(x)) \
       .saveAsTextFile(output_path)

    sc.stop()


if __name__ == '__main__':
    collect_files()
