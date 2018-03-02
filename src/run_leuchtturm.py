"""This module runs pipeline tasks in correct order."""

from settings import PATH_FILES_LISTED, PATH_PIPELINE_RESULTS, CLUSTER_PARALLELIZATION
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

    data = sc.textFile(PATH_FILES_LISTED, minPartitions=CLUSTER_PARALLELIZATION)

    data = split_emails(data)
    data = extract_metadata(data)
    data = deduplicate_emails(data)
    # data = clean_bodies(data)
    # data = extract_topics(data)
    # data = detect_languages(data)
    # data = extract_entities(data)

    data.saveAsTextFile(PATH_PIPELINE_RESULTS)

    sc.stop()


if __name__ == '__main__':
    run_email_pipeline()
