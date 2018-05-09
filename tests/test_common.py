"""Test common package."""

import pytest

from src.common import SparkProvider, Pipe, Pipeline
from config.config import Config


def test_spark_parallelism():
    """Parallelism level depends on env variable."""
    conf = Config(['--settings-run-distributed', '--spark-parallelism=276'])
    default = SparkProvider.spark_parallelism(conf)
    assert default == conf.get('spark', 'parallelism')

    conf = Config(['--settings-run-local', '--spark-parallelism=1'])
    default_cluster = SparkProvider.spark_parallelism(conf)
    assert default_cluster == 1


def test_spark_conf():
    """Spark config provider actually returns a valid conf."""
    config = Config([])
    conf = SparkProvider.spark_conf(config)
    assert conf is not None


def test_pyfiles():
    """Pyfiles returns a list of files."""
    files = SparkProvider.py_files()
    assert len(files) > 0
    assert 'src/common.py' in files


def test_pipeline_initializes():
    """Pipeline object initializes wo. raising exceptions."""
    pipeline = Pipeline(str(), str(), str())
    assert pipeline is not None


def test_pipeline_init_exception():
    """Pipeline object raises error when not initialized with all params."""
    with pytest.raises(TypeError):
        Pipeline()


def test_pipe_initializes():
    """Pipe object initializes wo. raising an exception."""
    conf = Config([])
    pipe = Pipe(conf)
    assert pipe is not None
