"""Common classes and functions to leuchtturm pipes and pipelines."""

from os import environ
from glob import glob

from pyspark import SparkContext, SparkConf


class SparkProvider(object):
    """Provides spark contet and environment configs such as parallelism level."""

    _spark_context = None

    @staticmethod
    def spark_context(conf):
        """Spark context singleton."""
        if SparkProvider._spark_context is None:
            SparkProvider._spark_context = SparkContext(conf=SparkProvider.spark_conf(conf),
                                                        pyFiles=SparkProvider.py_files())

        return SparkProvider._spark_context

    @staticmethod
    def stop_spark_context():
        """Stop spark context once execution is done."""
        if SparkProvider._spark_context is not None:
            SparkProvider._spark_context.stop()

    @staticmethod
    def spark_conf(conf):
        """Provide config for spark context."""
        spark_conf = SparkConf().set('spark.hive.mapred.supports.subdirectories', 'true') \
            .set('spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive', 'true') \
            .set('spark.driver.memory', conf.get('spark', 'ram'))

        return spark_conf

    @staticmethod
    def py_files():
        """List all py files that must be included for execution."""
        return glob('src/**/*.py', recursive=True)

    @staticmethod
    def spark_parallelism(conf):
        """If running on cluster return high degree of parallelism."""
        if conf.get('spark', 'run_local'):
            return 1
        return conf.get('spark', 'parallelism')


class Pipeline(object):
    """Combine multiple pipes to a pipeline."""

    def __init__(self, reader, pipes, writer, validate_before_run=False):
        """Definition of pipeline. Reader, array of pipes, export pipe."""
        self.reader = reader
        self.pipes = pipes
        self.writer = writer
        self.validate_before_run = validate_before_run

    def validate(self):
        """Validate pipeline."""
        raise NotImplementedError

    def run(self):
        """Run pipeline."""
        corpus = self.reader.run()
        for pipe in self.pipes:
            corpus = pipe.run(corpus)
        self.writer.run(corpus)


class Pipe(object):
    """Meta class for all pipes. Defines API."""

    def __init__(self, conf):
        """Initialize common vars."""
        super().__init__()
        self.parallelism = SparkProvider.spark_parallelism(conf)

    def run_on_document(self, raw_message):
        """Run task on a single document. Should be unit tested."""
        raise NotImplementedError

    def run_on_partition(self, partition):
        """Run task on a single partition. For expensive imports."""
        raise NotImplementedError

    def run(self, rdd):
        """Run task in spark context. Unless export pipe: return rdd."""
        raise NotImplementedError
