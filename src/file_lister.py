"""This module can parse files to json dicts and dump a list of them to txt."""


import os
import json
import luigi
import io
from datetime import datetime


DATETIMESTAMP = datetime.now().strftime('%Y-%m-%d_%H:%M')


class FileLister(luigi.Task):
    """
    A class for parsing files to json dicts and dumping them to a list.

    Functionalities:
    * read all files of given dir with given ending
    * parse to json dicts
    * dump to one list in file on given dir
    """

    source_dir = luigi.Parameter(default="./../tests/example-txts/")
    target_dir = luigi.Parameter(default="./../tests/example-txts/")

    def find_files_in_dir(self, ending):
        """Given ending and path to dir as a string, return list of filenames."""
        files = []

        for f in os.listdir(os.fsencode(self.source_dir)):
            filename = os.fsdecode(f)
            if filename.endswith(ending):
                files.append(os.path.join(self.source_dir, filename))

        return files

    def list_files(self, ending):
        """Given ending and source dir, dump a list of all matching files in source dir as json objects."""
        found_files = self.find_files_in_dir(ending)

        with io.open(self.output().path, 'w', encoding='utf8') as outfile:
            for f in found_files:
                with io.open(f, 'r', encoding='utf8') as infile:
                    outfile.write(
                        json.dumps({"doc_id": os.path.basename(f).replace('.txt', ''),
                                    "raw": infile.read()},
                                   ensure_ascii=False) +
                        '\n')

    def output(self):
        """File the list of json objects in a txtfiles textfile."""
        # replace LocalTarget with HdfsTarget to make it run on Cluster
        return luigi.LocalTarget(self.target_dir +
                                 DATETIMESTAMP +
                                 '_txtfiles.txt')

    def run(self):
        """Run the listing in the Luigi Task."""
        self.list_files('.txt')
