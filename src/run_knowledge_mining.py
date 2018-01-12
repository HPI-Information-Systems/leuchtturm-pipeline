"""This module runs pipeline tasks in correct order."""

from knowledge_mining import split_email
from knowledge_mining import extract_metadata
from knowledge_mining import deduplicate_emails
from knowledge_mining import extract_body
from knowledge_mining import clean_entry
from knowledge_mining import detect_language
from knowledge_mining import extract_entities
from pyspark import SparkContext


input_path = 'hdfs://172.18.20.109/pipeline/files_listed'
output_path = 'hdfs://172.18.20.109/pipeline/pipeline_results'


def run_knowledge_mining_pipeline():
    """Run entire text processing pipeline.

    Requires: File listing.
    Arguments: none.
    Returns: void.
    """
    # TODO: config
    sc = SparkContext()

    data = sc.textFile(input_path)

    data = data.map(lambda x: split_email(x)) \
               .map(lambda x: extract_metadata(x)) \
               .map(lambda x: deduplicate_emails(x)) \
               .map(lambda x: extract_body(x)) \
               .map(lambda x: clean_entry(x)) \
               .map(lambda x: detect_language(x)) \
               .map(lambda x: extract_entities(x))

    data.saveAsTextFile(output_path)

    sc.stop()


if __name__ == '__main__':
    run_knowledge_mining_pipeline()
