"""Test common package."""

import pytest

from src.common import SparkProvider, Pipe, Pipeline


def test_spark_parallelism(monkeypatch):
    """Parallelism level depends on env variable."""
    default = SparkProvider.spark_parallelism()
    assert default == 276
    monkeypatch.setenv('LEUCHTTURM_RUNNER', 'LOCAL')
    default_cluster = SparkProvider.spark_parallelism()
    assert default_cluster == 1


def test_spark_conf():
    """Spark config provider actually returns a valid conf."""
    conf = SparkProvider.spark_conf()
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
    pipe = Pipe()
    assert pipe is not None
