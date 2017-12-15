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
<<<<<<< HEAD
import pysolr
=======
from dateutil import parser
import time
from hdfs import InsecureClient
>>>>>>> dev

# Set this variable to true if you want all tasks to be run again
# Default should be true
rerun = True

if (rerun):
    DATETIMESTAMP = datetime.now().strftime('%Y-%m-%d_%H-%M') + '_'
else:
    DATETIMESTAMP = ''

class FileLister(luigi.Task):
    """A task for parsing files from HDFS to json dicts and dumping them to a list."""

    source_dir = luigi.Parameter(default="/pipeline/raw_emails/mails/")

    def output(self):
        """Write a HDFS target with timestamp."""
        return luigi.contrib.hdfs.HdfsTarget('/pipeline/emails_concatenated/' +
                                             DATETIMESTAMP +
                                             'emails_concatenated.txt')

    def run(self):
        """Run the listing."""
        client = InsecureClient('http://b7689.byod.hpi.de:50070')
        with self.output().open('w') as outfile:
            for file in client.list(self.source_dir):

                with client.read(self.source_dir + file, encoding='utf-8') as reader:
                    outfile.write(
                        json.dumps({"doc_id": file.replace('.txt', ''),
                                    "raw": str(reader.read()) },
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

    header = re.compile(r'^[ ]*(?:From:.*[\n]?|To:.*[\n]?|Cc:.*[\n]?|Date:.*[\n]?|Sent:.*[\n]?|Subject:.*[\n]?)',
                        re.I | re.M)

    leading_spaces = re.compile(r'^(?:\n|\s|\t)+', re.M)

    def requires(self):
        """Expect raw email data."""
        return FileLister()

    def output(self):
        """Produce HDFS target with new field parts."""
        return luigi.contrib.hdfs.HdfsTarget('/pipeline/parts_detected/' +
                                             DATETIMESTAMP +
                                             'parts_detected.txt')

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

        parts = self.split_email(document['raw'])

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
        part_object['body'] = re.sub(self.header, ' ', part)

        return part_object


class MetadataExtractor(luigi.Task):
    """This bad boy gets all them metadatas bout y'alls emails."""

    def requires(self):
        """Expect raw email data."""
        return InlineEmailSplitter()

    def output(self):
        """Write a HDFS target with timestamp."""
        return luigi.contrib.hdfs.HdfsTarget('/pipeline/metadata_extracted/' +
                                             DATETIMESTAMP +
                                             'metadata_extracted.txt')

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
        def clean_subject_line(subject_line):
            return re.sub(r'(fw:|re:|aw:|fwd:) *', '', subject_line.lower())

        def unify_date(date_string):
            return time.mktime(parser.parse(date_string).timetuple())

        # this does not conver all senders, but has a very high accuracy for Enron data
        def clean_person(person):
            #regex for finding emails
            mailreg = re.compile(r'\b[\w.-]+?@\w+?\.\w+?\b')
            mail = "".join((mailreg.findall(person))[:1])

            # regex for finding names that are clearly written
            namereg = re.compile(r'[A-Z][a-zA-Z ]+')
            name = re.sub(r' [a-z]+',"","".join((namereg.findall(person))[:1]))

            # regex for names that are seperated by a comma and a newline
            anothernamereg = re.compile(r'[a-z]+,\n *[a-zA-Z]+')
            another_name = "".join(anothernamereg.findall(person)[:1]).replace("\n", "").replace(" ", "").replace(",", ", ").strip().title()

            return {"email": mail,
                    "name": another_name if another_name else name}

        def clean_recipients(recipients_string, type):
            recipients = []

            def split_recipients(string):
                recipients = []
                if ">," in string:
                    recipients = string.split(">,\n")
                else:
                    recipients = string.split(",\n")
                return recipients

            sep_rec_strings = split_recipients(recipients_string)

            for rec_string in sep_rec_strings:
                cleaned_person = clean_person(rec_string)
                cleaned_person["type"] = type
                recipients.append(cleaned_person)

            return recipients

        document = json.loads(data)
        document["header"] = {}
        message = email.message_from_string(document["raw"])
        for metainfo in message.keys():
            header_piece = ""
            if metainfo == "Subject":
                header_piece = clean_subject_line(message[metainfo])
                document["header"][metainfo] = header_piece

            elif metainfo == "From":
                header_piece = clean_person(message[metainfo])
                document["header"]["sender"] = header_piece
            elif metainfo == "Date":
                header_piece = unify_date(message[metainfo])
                document["header"][metainfo] = header_piece
            elif metainfo == "To" or metainfo == "Cc":
                header_piece = clean_recipients(message[metainfo], metainfo)
                document["header"]["recipients"] = header_piece

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
                                             'emailbody_extracted.txt')

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
        mail_text = document['raw']
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
                                             'emails_cleaned.txt')

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
        """Clean main body and parts of one entry."""
        document = json.loads(data)

        document['body'] = self.clean_text(document['body'].replace('\\n', '\n'))

        for index, part in enumerate(document['parts']):
            document['parts'][index]['body'] = self.clean_text(document['parts'][index]['body'].replace('\\n', '\n'))

        return json.dumps(document, ensure_ascii=False)

    def clean_text(self, text):
        """Clean an email text."""
        body_cleaned = text
        for rule in self.rules:
            body_cleaned = re.sub(rule, ' ', body_cleaned)

        body_cleaned, temp = extract_signature(body_cleaned)
        for sc in self.special_chars:
            body_cleaned = body_cleaned.replace(sc, ' ')

        return re.sub(r'(\n|\t| )+', ' ', body_cleaned)


class LanguageDetector(luigi.Task):
    """This task detects the language of a text using langdetect."""

    def requires(self):
        """Require e-mail text without headers and footer."""
        return EmailCleaner()

    def output(self):
        """Write a HDFS target with timestamp."""
        return luigi.contrib.hdfs.HdfsTarget('/pipeline/language_detected/' +
                                             DATETIMESTAMP +
                                             'language_detected.txt')

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
        try:
            document['lang'] = detect(document['body'])
        except Exception as e:
            document['lang'] = 'unknown'
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
                                             'entities_counted.txt')

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
        """Extract entities from each document into entities object."""
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
        """JSON Object defninition for entity."""
        return {
            "entity": entity,
            "entity_type": entity_type,
            "entity_count": entity_count
        }


class CreateValidJson(luigi.Task):
        """This task creates valid JSON - nice."""

        def requires(self):
            """Require finished pipeline."""
            return EntityExtractorAndCounter()

        def output(self):
            """Write a HDFS target with timestamp."""
            return luigi.contrib.hdfs.HdfsTarget('/pipeline/json_created/' +
                                                 DATETIMESTAMP +
                                                 'json_created.txt')

        def run(self):
            """Run json creation."""

            sc = SparkContext()
            data = sc.textFile(self.input().path)
            results = data.collect()
            with self.output().open('w') as f:
                f.write('[' + '\n')
                lastResult = results.pop()
                for result in results:
                    f.write(result + ',' + '\n')
                f.write(lastResult + '\n')
                f.write(']')
            sc.stop()


class WriteToSolr(luigi.WrapperTask):
    """Write  to a solr core."""

    solr = pysolr.Solr('http://b1184.byod.hpi.de:8983/solr/entities', timeout=10)

    def requires(self):
        """Require last task of pipeline."""
        return EntityExtractorAndCounter()

    def run(self):
        """Iterate over documents and add them to solr db."""
        sc = SparkContext()
        documents = sc.textFile(self.input().path).collect()

        for document in documents:
            print(document)
            document = self.transform_lists_to_objects(json.loads(document))
            self.add_document_to_solr(document)

        sc.stop()

    def transform_lists_to_objects(self, document):
        # entities to object
        for type in document["entities"].keys():
            document["entities"][type] = dict(enumerate(document["entities"][type]))

        # parts to object
        document["parts"] = dict(enumerate(document["parts"]))

        # recipients to object
        if "header" in document.keys():
            document["header"]["recipients"] = dict(enumerate(document["header"]["recipients"]))

        return document

    def add_document_to_solr(self, document):
        """Add a single document to solr db."""
        self.solr.add([document])
