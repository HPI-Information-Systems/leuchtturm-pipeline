"""This module runs pipeline tasks in correct order."""

from settings import file_lister_path_spark, pipeline_result_path_spark, cluster_parallelization
from leuchtturm import split_emails
from leuchtturm import extract_metadata
from leuchtturm import deduplicate_emails
from leuchtturm import clean_bodies
from leuchtturm import detect_languages
from leuchtturm import extract_entities
from pyspark import SparkContext


def run_email_pipeline():
    """Run entire text processing pipeline.

    Requires: File listing.
    Arguments: none.
    Returns: void.
    """
    sc = SparkContext()

    data = sc.textFile(file_lister_path_spark, minPartitions=cluster_parallelization)

    data = split_emails(data)
    data = extract_metadata(data)
    data = deduplicate_emails(data)
    data = clean_bodies(data)
    data = detect_languages(data)
    data = extract_entities(data)

    data.saveAsTextFile(pipeline_result_path_spark)

    sc.stop()


if __name__ == '__main__':
    run_email_pipeline()
