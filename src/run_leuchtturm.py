"""This module runs pipeline tasks in correct order."""

from settings import PATH_FILES_LISTED, PATH_PIPELINE_RESULTS
from leuchtturm import (split_emails, extract_metadata, deduplicate_emails,
                        clean_bodies, detect_languages, extract_entities, extract_topics)
import sys
from pyspark import SparkContext


def run_email_pipeline(input_path=PATH_FILES_LISTED, output_path=PATH_PIPELINE_RESULTS):
    """Run entire text processing pipeline.

    Requires: File listing.
    Arguments: none.
    Returns: void.
    """
    sc = SparkContext()

    data = sc.textFile(input_path)

    data = split_emails(data)
    data = extract_metadata(data)
    data = deduplicate_emails(data)
    data = clean_bodies(data)
    data = extract_topics(data)
    data = detect_languages(data)
    data = extract_entities(data)

    data.saveAsTextFile(output_path)

    sc.stop()


if __name__ == '__main__':
    run_email_pipeline(input_path=sys.argv[1], output_path=sys.argv[2])
