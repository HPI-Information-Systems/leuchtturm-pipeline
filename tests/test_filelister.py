"""This module contains all tests for FileLister."""

from src.file_lister import FileLister
import os
import codecs


def test_find_files_in_dir():
    """Assert that file finder only selects txts."""
    luigi_task = FileLister(source_dir='./tests/example-txts/',
                            target_dir='./tests/example-txts/')

    txts = luigi_task.find_files_in_dir('.txt')
    assert len(txts) == 3

    pdfs = luigi_task.find_files_in_dir('.pdf')
    assert len(pdfs) == 0


def test_run():
    """Assert luigi output is being produced and contains all example files."""
    luigi_task = FileLister(source_dir='./tests/example-txts/',
                            target_dir='./tests/example-txts/')

    files = luigi_task.find_files_in_dir('.txt')
    luigi_task.run()

    assert len(os.listdir('./tests/example-txts/')) == len(files) + 1

    with codecs.open(luigi_task.output().path, 'r', encoding='utf8') as outfile:
        outfile_str = outfile.read()
        assert 'On Unix systems (Linux, Mac OS X, etc.), binary' in outfile_str
        assert 'Jährlich verschleudert Amazon in der Cyber Monday Woche' in outfile_str
        assert 'On ne connaît de lui que son pseudo, le générique' in outfile_str
