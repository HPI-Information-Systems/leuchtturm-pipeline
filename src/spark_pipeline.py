"""This module processes emails."""


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
import re
from talon.signature.bruteforce import extract_signature


DATETIMESTAMP = datetime.now().strftime('%Y-%m-%d_%H-%M')


class FileLister(luigi.Task):
    """A task for parsing files to json dicts and dumping them to a list."""

    source_dir = luigi.Parameter(default="./../tests/example-txts/")

    def output(self):
        """Write a HDFS target with timestamp."""
        return luigi.contrib.hdfs.HdfsTarget('/pipeline/emails_concatenated/' +
                                             DATETIMESTAMP +
                                             '_emails_concatenated.txt')

    def run(self):
        """Run the listing."""
        self.list_files('.txt')

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


class MetadataExtractor(luigi.Task):
    """This bad boy gets all them metadatas bout y'alls emails."""

    def requires(self):
        """Expect raw email data."""
        return FileLister()

    def output(self):
        """Write a HDFS target with timestamp."""
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
        document = json.loads(input)
        message = email.message_from_string(document["full_body"])
        for metainfo in message.keys():
            document[metainfo] = message[metainfo]

        return json.dumps(document, ensure_ascii=False)


class EmailBodyExtractor(luigi.Task):
    """This bad boy gets all them email booti bout y'alls emails."""

    def requires(self):
        """Expect raw email data."""
        return MetadataExtractor()

    def output(self):
        """Write a HDFS target with timestamp."""
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
        document = json.loads(input)
        mail_text = document['full_body']
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

        document['body'] = body_text

        return json.dumps(document, ensure_ascii=False)


class EmailCleaner(luigi.Task):
    """This task uses Talon from Mailgun to clean emails."""

    special_chars = [
        '"', "!", "#", "$", "%", "&", "'", "§", "(", ")", "*", "+",
        "-", ".", "/", ":", ";", "<", "=", ">", "?",
        "@", "[", "\\", "]", "^", "_", "`", "{", "|", "}", "~", "\n", "\u000b", "\f"
        ]

    rules = [
        r'----- Forwarded(.)*(From:(.)*|Subject:(.)*|To:(.)*|Sent:(.)*|Cc:(.)*|\n)*\n',
        r'-----Original Message-----(From:(.)*|Subject:(.)*|To:(.)*|Sent:(.)*|Cc:(.)*|\n)*\n',
        r'((From:)(.)*(\n)*)?To:(.)*(\n)*cc:(.)*(\n)*Subject:(.)*(\n)*',
        r'\*\*\*\*\*\*\*\*\*\*\*(\n)+(.)*(\n)+\*\*\*\*\*\*\*\*\*\*\*'
        ]

    def requires(self):
        """Require FileLister like file with field: body."""
        return EmailBodyExtractor()

    def output(self):
        """Write a HDFS target with timestamp."""
        return luigi.contrib.hdfs.HdfsTarget('/pipeline/emails_cleaned/' +
                                             DATETIMESTAMP +
                                             '_emails_cleaned.txt')

    def run(self):
        """Run cleansing task."""
        sc = SparkContext()
        data = sc.textFile(self.input().path)
        results = data.map(lambda x: self.clean_entry(x)).collect()
        with self.output().open('w') as f:
            for result in results:
                f.write(result + '\n')
        sc.stop()

    def clean_entry(self, input):
        """Clean one entry."""
        document = json.loads(input)

        # run rules
        body_cleaned = document['body'].replace('\\n', '\n')
        for rule in self.rules:
            body_cleaned = re.sub(rule, ' ', body_cleaned)

        # run talon
        body_cleaned, temp = extract_signature(body_cleaned)

        # remove special chars and whitespace
        for sc in self.special_chars:
            body_cleaned = body_cleaned.replace(sc, ' ')
        body_cleaned = re.sub(r'(\n|\t| )+', ' ', body_cleaned)

        document['body'] = body_cleaned

        return json.dumps(document, ensure_ascii=False)


class LanguageDetector(luigi.Task):
    """This task detects the language of a text using langdetect."""

    def requires(self):
        """Require e-mail text without headers and footer."""
        return EmailCleaner()

    def output(self):
        """Write a HDFS target with timestamp."""
        return luigi.contrib.hdfs.HdfsTarget('/pipeline/language_detected/' +
                                             DATETIMESTAMP +
                                             '_language_detected.txt')

    def run(self):
        """Run language detection."""
        sc = SparkContext()
        data = sc.textFile(self.input().path)
        results = data.map(lambda x: self.detect_language(x)).collect()
        with self.output().open('w') as f:
            for result in results:
                f.write(result + '\n')
        sc.stop()

    def detect_language(self, input):
        """Add language to each entry."""
        document = json.loads(input)
        document['lang'] = detect(document['full_body'])
        return json.dumps(document, ensure_ascii=False)
