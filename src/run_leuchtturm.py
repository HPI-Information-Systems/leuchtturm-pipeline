"""This module runs pipeline tasks in correct order."""

from settings import path_files_listed, path_pipeline_results, cluster_parallelization
from leuchtturm import (split_emails, extract_metadata, deduplicate_emails,
                        clean_bodies, detect_languages, extract_entities, extract_topics)
from pyspark import SparkContext


def run_email_pipeline():
    """Run entire text processing pipeline.

    Requires: File listing.
    Arguments: none.
    Returns: void.
    """
    sc = SparkContext()

    data = sc.textFile(path_files_listed, minPartitions=cluster_parallelization)

    data = split_emails(data)
    data = extract_metadata(data)
    data = deduplicate_emails(data)
    data = clean_bodies(data)
    data = extract_topics(data)
    data = detect_languages(data)
    data = extract_entities(data)

    data.saveAsTextFile(path_pipeline_results)

    sc.stop()


if __name__ == '__main__':
    run_email_pipeline()
