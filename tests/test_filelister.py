# """This module contains all tests for FileLister."""

# from src.file_lister import FileLister
# import os
# import codecs


# def test_find_files_in_dir():
#     """Assert that file finder only selects txts."""
#     luigi_task = FileLister(source_dir='./tests/example-txts/',
#                             target_dir='./tests/')

#     txts = luigi_task.find_files_in_dir('.txt')
#     assert len(list(txts)) == 3

#     pdfs = luigi_task.find_files_in_dir('.pdf')
#     assert len(list(pdfs)) == 0


# def test_run():
#     """Assert luigi output is being produced and contains all example files."""
#     luigi_task = FileLister(source_dir='./tests/example-txts/',
#                             target_dir='./tests/')

#     luigi_task.run()

#     assert os.path.isfile(luigi_task.output().path)

#     with codecs.open(luigi_task.output().path, 'r', encoding='utf8') as outfile:
#         outfile_str = outfile.read()
#         assert 'On Unix systems (Linux, Mac OS X, etc.), binary' in outfile_str
#         assert 'Jährlich verschleudert Amazon in der Cyber Monday Woche' in outfile_str
#         assert 'On ne connaît de lui que son pseudo, le générique' in outfile_str

#     os.remove(luigi_task.output().path)
