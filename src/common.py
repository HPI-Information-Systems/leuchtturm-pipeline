"""Common classes and functions to leuchtturm pipes and pipelines."""

from os import environ
from glob import glob

from pyspark import SparkContext, SparkConf, serializers


class SparkProvider(object):
    """Provides spark contet and environment configs such as parallelism level."""

    _spark_context = None

    @staticmethod
    def spark_context():
        """Spark context singleton."""
        if SparkProvider._spark_context is None:
            SparkProvider._spark_context = SparkContext(conf=SparkProvider.spark_conf(),
                                                        pyFiles=SparkProvider.py_files(),
                                                        serializer=serializers.MarshalSerializer())

        return SparkProvider._spark_context

    @staticmethod
    def stop_spark_context():
        """Stop spark context once execution is done."""
        if SparkProvider._spark_context is not None:
            SparkProvider._spark_context.stop()

    @staticmethod
    def spark_conf():
        """Provide config for spark context."""
        conf = SparkConf().set('spark.hive.mapred.supports.subdirectories', 'true') \
                          .set('spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive', 'true')

        return conf

    @staticmethod
    def py_files():
        """List all py files that must be included for execution."""
        return glob('src/**/*.py', recursive=True)

    @staticmethod
    def is_in_clustermode():
        """Check if env variabke LEUCHTTURM_RUNNER is set to CLUSTER."""
        try:
            if environ['LEUCHTTURM_RUNNER'] == 'LOCAL':
                return False
        except KeyError:
            return True

    @staticmethod
    def spark_parallelism():
        """If running on cluster return high degree of parallelism."""
        if SparkProvider.is_in_clustermode():
            return 276
        else:
            return 1
