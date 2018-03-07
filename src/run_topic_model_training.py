"""This module runs the model training pipeline."""

from settings import PATH_FILES_LISTED, PATH_LDA_MODEL
from leuchtturm import (extract_metadata,
                        clean_bodies, train_topic_model)
import sys
from pyspark import SparkContext, SparkConf
import os.path
import json


def run_topic_training(input_path=PATH_FILES_LISTED, output_path=PATH_PIPELINE_RESULTS):
    """Run entire text processing pipeline.

    Requires: File listing.
    Arguments: none.
    Returns: void.
    """
    config = SparkConf().set('spark.hive.mapred.supports.subdirectories', 'true') \
                        .set('spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive', 'true') \
                        .set('spark.default.parallelism', 120) \
                        .set('spark.logConf', True) \
                        .set('spark.logLevel', 'ERROR') \
                        .set('spark.yarn.maxAppAttempts', 1)

    sc = SparkContext(conf=config)

    data = sc.textFile(input_path)

    data = extract_metadata(data)
    data = clean_bodies(data)
    data = train_topic_model(data)

    data.saveAsTextFile(output_path)

    sc.stop()


if __name__ == '__main__':
    if not os.path.isfile(PATH_LDA_Model): 
        run_topic_training(input_path=sys.argv[1], output_path=sys.argv[2])
