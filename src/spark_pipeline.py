"""This module can parse files to json dicts and dump a list of them to txt."""


import os
import json
import luigi
import luigi.contrib.hdfs
import findspark
findspark.init('/usr/hdp/2.6.3.0-235/spark2')
from pyspark import SparkContext
import numpy as np
from datetime import datetime
import emailbody.extractmailbody as body_extractor
from langdetect import detect
import email as email

import talon
talon.init()
from talon import quotations
from talon.signature.bruteforce import extract_signature



DATETIMESTAMP = datetime.now().strftime('%Y-%m-%d_%H-%M')


class FileLister(luigi.Task):
    """
    A class for parsing files to json dicts and dumping them to a list.

    Functionalities:
    * read all files of given dir with given ending
    * parse to json dicts
    * dump to one list in file on given dir
    """

    source_dir = luigi.Parameter(default="./../tests/example-txts/")

    def find_files_in_dir(self, ending):
        """Given ending and path to dir as a string, return list of filenames."""
        for f in os.listdir(os.fsencode(self.source_dir)):
            filename = os.fsdecode(f)
            if filename.endswith(ending):
                yield os.path.join(self.source_dir, filename)

    def list_files(self, ending):
        """Given ending and source dir, dump a list of all matching files in source dir as json objects."""
        found_files = self.find_files_in_dir(ending)

        with self.output().open('w') as outfile:
            for f in found_files:
                with open(f, 'r', encoding='utf8') as infile:
                    outfile.write(
                        json.dumps({"doc_id": os.path.basename(f).replace('.txt', ''),
                                    "full_body": infile.read()},
                                   ensure_ascii=False) +
                        '\n')

    def output(self):
        """File the list of json objects in a txtfiles textfile."""
        return luigi.contrib.hdfs.HdfsTarget('/pipeline/emails_concatenated/' +
                                             DATETIMESTAMP +
                                             '_emails_concatenated.txt')

    def run(self):
        """Run the listing in the Luigi Task."""
        self.list_files('.txt')


class EnronFooterRemover(luigi.Task):
    """Given a filepath, this task removes the footer that was added after the initial export of the data set."""

    footer = ('***********\nEDRM Enron Email Data Set has been produced in EML, PST and NSF format by ZL '
              'Technologies, Inc. This Data Set is licensed under a Creative Commons Attribution 3.0 United '
              'States License <http://creativecommons.org/licenses/by/3.0/us/> . To provide attribution, please '
              'cite to \"ZL Technologies, Inc. (http://www.zlti.com).\"\n***********\n')

    def requires(self):
        """Require e-mail text without headers."""
        return FileLister()

    def output(self):
        """Override file at given path without footer."""
        return luigi.contrib.hdfs.HdfsTarget('/pipeline/footer_removed/' +
                                             DATETIMESTAMP +
                                             '_footer_removed.txt')

    def run(self):
        """Execute footer removal."""
        sc = SparkContext()
        data = sc.textFile(self.input().path)
        #result = data.map(lambda x: self.remove_footer(x)).saveAsTextFile(self.output().path)
        results = data.map(lambda x: self.remove_footer(x)).collect()
        with self.output().open('w') as f:
            for result in results:
                f.write(result + '\n')
        sc.stop()

    def remove_footer(self, input):
        """Replace footer with empty string."""
        dict = json.loads(input)
        dict['body'] = dict['full_body'].replace(self.footer, '')
        return json.dumps(dict, ensure_ascii=False)


class MetadataExtractor(luigi.Task):
    """This bad boy gets all them metadatas bout y'alls emails."""

    def requires(self):
        """Raw email data."""
        return EnronFooterRemover()

    def output(self):
        """Add metadata to each mail."""
        return luigi.contrib.hdfs.HdfsTarget('/pipeline/metadata_extracted/' +
                                             DATETIMESTAMP +
                                             '_metadata_extracted.txt')

    def run(self):
        """Excecute meta data extraction."""
        sc = SparkContext()
        data = sc.textFile(self.input().path)
        results = data.map(lambda x: self.extract_metadata(x)).collect()
        with self.output().open('w') as f:
            for result in results:
                f.write(result + '\n')
        sc.stop()

    def extract_metadata(self, input):
        """Extract meta data of each email."""
        dict = json.loads(input)
        message = email.message_from_string(dict["full_body"])
        for metainfo in message.keys():
            dict[metainfo] = message[metainfo]

        return json.dumps(dict, ensure_ascii=False)


class EmailBodyExtractor(luigi.Task):
    """This bad boy gets all them email booti bout y'alls emails."""

    def requires(self):
        """Raw email data."""
        return MetadataExtractor()

    def output(self):
        """Add pure body to each mail."""
        return luigi.contrib.hdfs.HdfsTarget('/pipeline/emailbody_extracted/' +
                                             DATETIMESTAMP +
                                             '_emailbody_extracted.txt')

    def run(self):
        """Excecute body extraction."""
        sc = SparkContext()
        data = sc.textFile(self.input().path)
        results = data.map(lambda x: self.get_body(x)).collect()
        with self.output().open('w') as f:
            for result in results:
                f.write(result + '\n')
        sc.stop()

    def get_body(self, input):
        """Extract and return the body of an email."""
        dict = json.loads(input)
        mail_text = dict['full_body']
        text_lines = mail_text.splitlines()

        func_b = body_extractor.enron_two_zone_line_b_func
        model = body_extractor.enron_two_zone_model

        text_embedded = body_extractor.embed(text_lines, [func_b])
        head_body_predictions = model.predict(np.array([text_embedded])).tolist()[0]

        head_body_indicator = list(zip(text_lines, head_body_predictions))

        body_text = ''
        for i in range(0, len(head_body_indicator)):
            if int(head_body_indicator[i][1][0]) == int(1.0):
                body_text += head_body_indicator[i][0]

        dict['body'] = body_text

        return json.dumps(dict, ensure_ascii=False)


class EmailCleaner(luigi.Task):
    """This task uses Talon from Mailgun to clean emails."""

    def requires(self):
        """Dependency: FileLister like file with field: body."""
        return EmailBodyExtractor()

    def output(self):
        """Write a HDFS target with timestamp."""
        return luigi.contrib.hdfs.HdfsTarget('/pipeline/emails_cleaned/' +
                                             DATETIMESTAMP +
                                             '_emails_cleaned.txt')

    def run(self):
        """Run luigi task."""
        sc = SparkContext()
        data = sc.textFile(self.input().path)
        results = data.map(lambda x: self.clean_entry(x)).collect()
        with self.output().open('w') as f:
            for result in results:
                f.write(result + '\n')
        sc.stop()

    def clean_entry(self, entry):
        """Read body, and clean it of noise using Talon."""
        dict = json.loads(entry)
        body = dict['body'].replace('\\n', '\n')
        reply = quotations.extract_from_plain(body)
        text, signat = extract_signature(reply)
        dict['reply'] = reply
        dict['reply_text'] = text
        # dict['signature'] = signat

        return json.dumps(dict, ensure_ascii=False)


class LanguageDetector(luigi.Task):
    """
    Given a filepath, this task extracts the body of an e-mail and writes it to a file at the target path.

    This job uses Quagga by Tim Repke.
    """

    def requires(self):
        """Require e-mail text without headers and footer."""
        return EmailCleaner()

    def detect_language(self, input):
        """Add language to each entry."""
        dict = json.loads(input)
        dict['lang'] = detect(dict['full_body'])
        return json.dumps(dict, ensure_ascii=False)

    def run(self):
        """Replace footer with empty string."""
        sc = SparkContext()
        data = sc.textFile(self.input().path)
        results = data.map(lambda x: self.detect_language(x)).collect()
        with self.output().open('w') as f:
            for result in results:
                f.write(result + '\n')
        sc.stop()

    def output(self):
        """Override file at given path without footer."""
        return luigi.contrib.hdfs.HdfsTarget('/pipeline/language_detected/' +
                                             DATETIMESTAMP +
                                             '_language_detected.txt')
