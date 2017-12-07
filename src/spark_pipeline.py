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


class InlineEmailSplitter(luigi.Task):
    """This task splits replies and forwards of an email into separate parts."""

    splitters = [
        # ----------- Forwarded by Max Mustermann on 24 Jan 2001 ------------
        re.compile(r'[\s]*[-]+[ ]*Forwarded .*[ ]*[-]+', re.I),
        # Thu, 24 Jun 2001 01:00:51 +0000 Max Mustermann <max@mustermann.com>:
        re.compile(r'\S{3,10}, \d\d? \S{3,10} 20\d\d,? \d\d?:\d\d(:\d\d)?( \S+){3,6}@\S+:', re.I | re.M),
        # ---- Max Mustermann wrote ----
        re.compile(r'[\s]*[-]+.*(?:wrote|sent)[ ]*[-]+', re.I),
        # ------ Original Message ------
        re.compile(r'[\s]*[-]+[ ]*(?:Original Message|Reply Message)[ ]*[-]+', re.I),
        # On Date, Max Mustermann wrote:
        re.compile(r'(-*[>]?[ ]?On[ ].*,(.*\n){{0,2}}.*wrote:?-*)', re.I)
        ]

    header = re.compile(r'^[ ]*(?:From:.*[\n]?|To:.*[\n]?|Cc:.*[\n]?|Date:.*[\n]?|Sent:.*[\n]?|Subject:.*[\n]?)', re.I | re.M)

    leading_spaces = re.compile(r'^(?:\n|\s|\t)+', re.M)

    def requires(self):
        """Expect raw email data."""
        return FileLister()

    def output(self):
        """Produce HDFS target with new field parts."""
        return luigi.contrib.hdfs.HdfsTarget('/pipeline/email_parts/' +
                                             DATETIMESTAMP +
                                             '_parts_detected.txt')

    def run(self):
        """Execute splitting."""
        sc = SparkContext()
        data = sc.textFile(self.input().path)
        results = data.map(lambda x: self.process_email(x)).collect()
        with self.output().open('w') as f:
            for result in results:
                f.write(result + '\n')
        sc.stop()

    def process_email(self, data):
        """Split email into parts and extract metadata."""
        document = json.loads(data)

        parts = self.split_email(document['full_body'])

        part_objects = []
        for part in parts:
            part_objects.append(self.extract_metadata(part))

        document['parts'] = part_objects
        return json.dumps(document, ensure_ascii=False)

    def split_email(self, email):
        """Split email into parts and return list of parts."""
        parts = np.array([email])
        for pattern in self.splitters:
            new_parts = np.array([])
            for part in parts:
                current_part = re.split(pattern, part)
                new_parts = np.append(new_parts, current_part)
            parts = new_parts.flatten()

        return parts

    def extract_metadata(self, part):
        """Extract metadata of one email part and return is at dictionary."""
        part = re.sub(self.leading_spaces, '', part)
        part_object = {}
        part_object['header'] = {}
        meta = email.message_from_string(part)
        for meta_element in meta.keys():
            part_object['header'][meta_element] = meta[meta_element]
        part_object['text'] = re.sub(self.header, ' ', part)

        return part_object


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

    def extract_metadata(self, data):
        """Extract meta data of each email."""
        document = json.loads(data)
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

    def get_body(self, data):
        """Extract and return the body of an email."""
        document = json.loads(data)
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
        r'\*\*\*\*\*\*\*\*\*\*\*(\n)?EDRM Enron Email(.)*(\n)?\*\*\*\*\*\*\*\*\*\*\*',
        r'----- Forwarded(.)*(From:(.)*|Subject:(.)*|To:(.)*|Sent:(.)*|Cc:(.)*|\n)*\n',
        r'-----Original Message-----(From:(.)*|Subject:(.)*|To:(.)*|Sent:(.)*|Cc:(.)*|\n)*\n',
        r'((From:)(.)*(\n)*)?To:(.)*(\n)*cc:(.)*(\n)*Subject:(.)*(\n)*',
        r'={76}(.|\n)*={76}'
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

    def clean_entry(self, data):
        """Clean one entry."""
        document = json.loads(data)

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

    def detect_language(self, data):
        """Add language to each entry."""
        document = json.loads(data)
        document['lang'] = detect(document['full_body'])
        return json.dumps(document, ensure_ascii=False)
