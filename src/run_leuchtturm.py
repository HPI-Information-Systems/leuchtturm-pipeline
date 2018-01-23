"""This module runs pipeline tasks in correct order."""

from leuchtturm import split_emails
from leuchtturm import extract_metadata
from leuchtturm import deduplicate_emails
from leuchtturm import clean_bodies
from leuchtturm import detect_languages
from leuchtturm import extract_entities
from pyspark import SparkContext


input_path = 'hdfs://172.18.20.109/LEUCHTTURM/files_listed_nuix'
output_path = 'hdfs://172.18.20.109/LEUCHTTURM/pipeline_results_nuix'


def run_email_pipeline():
    """Run entire text processing pipeline.

    Requires: File listing.
    Arguments: none.
    Returns: void.
    """
    sc = SparkContext()

    # set minPartitions to executors * cores per executor * 3
    data = sc.textFile(input_path, minPartitions=54)

    data = split_emails(data)
    data = extract_metadata(data)
    data = deduplicate_emails(data)
    data = clean_bodies(data)
    data = detect_languages(data)
    data = extract_entities(data)

    data.saveAsTextFile(output_path)

    sc.stop()


if __name__ == '__main__':
    run_email_pipeline()
