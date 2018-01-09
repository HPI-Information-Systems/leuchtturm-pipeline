"""This module runs pipeline tasks in correct order."""

from .knowledge_mining import split_email
from .knowledge_mining import extract_metadata
from .knowledge_mining import deduplicate_emails
from .knowledge_mining import extract_body
from .knowledge_mining import clean_entry
from .knowledge_mining import detect_language
from .knowledge_mining import extract_entities
import findspark
findspark.init('/usr/hdp/2.6.3.0-235/spark2')
from pyspark import SparkContext


input_path = "/path/to/files/listed/data-frame"
output_path = "/path/to/output/data-frame"


"""Run entire text processing pipeline.

Requires: File listing.
Arguments: none.
Returns: void.
"""
def run_knowledge_mining_pipeline():
    """Read listed files into spark context and start pipeline."""
    # TODO: config
    sc = SparkContext()

    data = sc.textFile(input_path)

    data = data.map(lambda x: split_email(x))
    data = data.map(lambda x: extract_metadata(x))
    data = data.map(lambda x: deduplicate_emails(x))
    data = data.map(lambda x: extract_body(x))
    data = data.map(lambda x: clean_entry(x))
    data = data.map(lambda x: detect_language(x))
    data = data.map(lambda x: extract_entities(x))

    data.saveAsTextFile(output_path)

    sc.stop()


if __name__ == "__main__":
    run_knowledge_mining_pipeline()
