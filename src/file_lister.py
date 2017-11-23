"""This module can parse files to json dicts and dump a list of them to txt."""


import os
import json
import luigi

SOURCE_DIR = "example-txts"
TARGET_DIR = "example-txts"


class FileLister(luigi.Task):
    """
    A class for parsing files to json dicts and dumping them to a list.

    Functionalities:
    * read all files of given dir with given ending
    * parse to json dicts
    * dump to one list in file on given dir
    """

    def find_files_in_dir(self, ending, source_dir):
        """Given ending and path to dir as a string, return list of filenames."""
        directory = os.fsencode(source_dir)
        files = []

        for f in os.listdir(directory):
            filename = os.fsdecode(f)
            if filename.endswith(ending):
                files.append(filename)

        return files

    def parse_file_to_json(self, file):
        """Encode file as json string."""
        dict_string = "{'raw':'"
        dict_string += file.read()
        dict_string += "'}"
        json_string = json.dumps(dict_string)
        return json_string

    def list_files(self, ending, source_dir, target_dir):
        """Given ending and source dir, dump a list of all matching files in source dir as json objects."""
        found_files = self.find_files_in_dir(ending, source_dir)

        with open(target_dir + '/txtfiles.txt', 'w') as outfile:
            for f in found_files:
                with open(f) as infile:
                    json_string = self.parse_file_to_json(infile)
                    outfile.write(json_string + '\n')

    def output(self):
        """File the list of json objects in a txtfiles textfile."""
        return luigi.LocalTarget(TARGET_DIR + '/txtfiles.txt')

    def run(self):
        """Run the listing in the Luigi Task."""
        self.list_files('.txt', SOURCE_DIR, TARGET_DIR)
