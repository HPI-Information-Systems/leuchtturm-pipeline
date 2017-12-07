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
import spacy

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


class EntityExtractorAndCounter(luigi.Task):
    """Extract entities with spacy and count them."""
    nlp = spacy.load('en')

    def requires(self):
        """Require the EmailPreprocessor of the preprocessing module."""
        return LanguageDetector()

    def output(self):
        """File the counted entities in a counting_done textfile."""
        return luigi.contrib.hdfs.HdfsTarget('/pipeline/entities_counted/' +
                                             DATETIMESTAMP +
                                             '_entities_counted.txt')

    def run(self):
        """Run the extraction in the Luigi Task."""
        sc = SparkContext()
        data = sc.textFile(self.input().path)
        results = data.map(lambda x: self.extract_entities(x)).collect()
        with self.output().open('w') as f:
            for result in results:
                f.write(result + '\n')
        sc.stop()

    def extract_entities(self, data):
        """Extract entities from each document into entities object"""
        document = json.loads(data)
        doc = self.nlp(document['body'])
        extracted_entities = []
        for entity in doc.ents:
            # some entities have a lot of ugly whitespaces in the beginning/end of string
            stripped_text = entity.text.replace('\n', '\\n').strip()
            duplicate_found = False
            for existing_entity in extracted_entities:
                if existing_entity["entity"] == stripped_text:
                    duplicate_found = True
                    existing_entity["entity_count"] += 1
                    break
            if not duplicate_found:
                extracted_entities.append(
                    self.make_entity(stripped_text, entity.label_, 1)
                )
        # ADD ENTITIES, THEIR TYPE AND THEIR COUNT 'IN BULK' TO SOLR DATABASE
        entities = {}
        entities['PERSON'] = []
        entities['NORP'] = []
        entities['FAC'] = []
        entities['ORG'] = []
        entities['GPE'] = []
        entities['LOC'] = []
        entities['PRODUCT'] = []
        entities['EVENT'] = []
        entities['WORK_OF_ART'] = []
        entities['LAW'] = []
        entities['LANGUAGE'] = []
        entities['DATE'] = []
        entities['TIME'] = []
        entities['PERCENT'] = []
        entities['MONEY'] = []
        entities['QUANTITY'] = []
        entities['ORDINAL'] = []
        entities['CARDINAL'] = []
        for entity in extracted_entities:
            entity_type = str(entity['entity_type'])
            entity.pop('entity_type', None)
            entity_string = str(entity).replace("'", '"')
            entities[entity_type].append(entity_string)
        document['entities'] = entities
        return json.dumps(document, ensure_ascii=False)

    def make_entity(self, entity, entity_type, entity_count):
        """JSON Object defninition for entity"""
        return {
            "entity": entity,
            "entity_type": entity_type,
            "entity_count": entity_count
        }
