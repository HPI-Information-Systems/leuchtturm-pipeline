"""This module provides methods to processes emails and text."""


import json
import re
import numpy as np
import time
from dateutil import parser
import email
from langdetect import detect
import xx_ent_wiki_sm as spacy


def split_emails(rdd):
    """Split email into parts and extract metadata.

    Arguments: rdd with raw, doc_id field for each doc in json format
    Returns: rdd with body field for each doc in json format splitted by parts of conversation
    """
    def split_document(data):
        splitters = [  # ----------- Forwarded by Max Mustermann on 24 Jan 2001 ------------
                       re.compile(r'[\s]*[-]+[ ]*Forwarded .*[ ]*[-]+', re.I),
                       # Thu, 24 Jun 2001 01:00:51 +0000 Max Mustermann <max@mustermann.com>:
                       re.compile(r'\S{3,10}, \d\d? \S{3,10} 20\d\d,? \d\d?:\d\d(:\d\d)?( \S+){3,6}@\S+:', re.I | re.M),
                       # ---- Max Mustermann wrote ----
                       re.compile(r'[\s]*[-]+.*(?:wrote|sent)[ ]*[-]+', re.I),
                       # ------ Original Message ------
                       re.compile(r'[\s]*[-]+[ ]*(?:Original Message|Reply Message)[ ]*[-]+', re.I),
                       # On Date, Max Mustermann wrote:
                       re.compile(r'(-*[>]?[ ]?On[ ].*,(.*\n){{0,2}}.*wrote:?-*)', re.I)]

        def detect_parts(email):
            """Split email into parts and return list of parts."""
            parts = np.array([email])
            for pattern in splitters:
                new_parts = np.array([])
                for part in parts:
                    current_part = re.split(pattern, part)
                    new_parts = np.append(new_parts, current_part)
                parts = new_parts.flatten()

            return parts

        document = json.loads(data)

        parts = detect_parts(document['raw'])

        splitted_emails = []
        for part in parts:
            obj = {'doc_id': document['doc_id'],
                   'raw': document['raw'],
                   'body': part}
            splitted_emails.append(json.dumps(obj, ensure_ascii=False))

        return splitted_emails

    return rdd.flatMap(lambda x: split_document(x))


def extract_metadata(rdd):
    """Extract meta data of each email.

    Arguments: rdd with body field for each doc in json format
    Returns: rdd with header field for each doc in json format
    """
    def add_metadata(data):
        def clean_subject_line(subject_line):
            return re.sub(r'(fw:|re:|aw:|fwd:) *', '', subject_line.lower())

        def unify_date(date_string):
            return time.mktime(parser.parse(date_string).timetuple())

        def clean_person(person):
            # regex for finding emails
            mailreg = re.compile(r'\b[\w.-]+?@\w+?\.\w+?\b')
            mail = "".join((mailreg.findall(person))[:1])

            # regex for finding names that are clearly written
            namereg = re.compile(r'[A-Z][a-zA-Z ]+')
            name = re.sub(r' [a-z]+', "", "".join((namereg.findall(person))[:1]))

            # regex for names that are seperated by a comma and a newline
            anothernamereg = re.compile(r'[a-z]+,\n *[a-zA-Z]+')
            another_name = "".join(anothernamereg.findall(person)[:1]) \
                             .replace("\n", "").replace(" ", "").replace(",", ", ").strip().title()

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
        message = email.message_from_string(document["body"])
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

    return rdd.map(lambda x: add_metadata(x))


def deduplicate_emails(rdd):
    """Deduplicate emails. Recognize emails by their header.

    Arguments: rdd with raw, header field for each doc in json format
    Returns: rdd that no longer contains duplicates as defined in select_email()
    """
    def convert_to_tupel(data):
        data_norm = json.loads(data)
        splitting_keys = json.dumps([data_norm['header']['sender']['email'],
                                     data_norm['header']['Date'],
                                     data_norm['header']['Subject']],
                                    ensure_ascii=False)

        return (splitting_keys, data)

    def select_email(data1, data2):
        if (len(data1) > len(data2)):
            return data1
        else:
            return data2

    def revert_to_json(data):
        return data[1]

    return rdd.map(lambda x: convert_to_tupel(x)) \
              .reduceByKey(lambda x, y: select_email(x, y)) \
              .map(lambda x: revert_to_json(x))


def clean_bodies(rdd):
    """Extract email body of each email.

    Arguments: rdd with body field for each doc in json format
    Returns: rdd with a cleaned body field for each doc in json format
    """
    def process_document(data):
        special_chars = ['"', "!", "#", "$", "%", "&", "'", "§", "(", ")", "*",
                         "-", "/", ":", ";", "<", "=", ">", "?", "\x97", "+",
                         "@", "[", "\\", "]", "^", "_", "`", "{", "|", "}", "~", "\u000b", "\f"]

        rules = [r'\*\*\*\*\*\*\*\*\*\*\*(\n)?EDRM Enron Email(.)*(\n)?\*\*\*\*\*\*\*\*\*\*\*',
                 r'----- Forwarded(.)*(From:(.)*|Subject:(.)*|To:(.)*|Sent:(.)*|Cc:(.)*|\n)*\n',
                 r'-----Original Message-----(From:(.)*|Subject:(.)*|To:(.)*|Sent:(.)*|Cc:(.)*|\n)*\n',
                 r'((From:)(.)*(\n)*)?To:(.)*(\n)*cc:(.)*(\n)*Subject:(.)*(\n)*',
                 r'={60,}(.|\n)*={60,}']

        document = json.loads(data)
        mail_text = document['body']
        for rule in rules:
            mail_text = re.sub(rule, ' ', mail_text)

        for sc in special_chars:
            mail_text = mail_text.replace(sc, ' ')

        document['body'] = re.sub(r'(\n|\t| ){2,}', ' ', mail_text)
        return json.dumps(document, ensure_ascii=False)

    return rdd.map(lambda x: process_document(x))


def detect_languages(rdd):
    """Detect language of each email.

    Arguments: rdd with body (cleaned) field for each doc in json format
    Returns: rdd with lang field for each doc in json format ('xx' if unknown)
    """
    def detect_email_lang(data):
        document = json.loads(data)
        try:
            document['lang'] = detect(document['body'])
        except Exception:
            document['lang'] = 'xx'
        return json.dumps(document, ensure_ascii=False)

    return rdd.map(lambda x: detect_email_lang(x))


def extract_entities(rdd):
    """Extract entities of each email.

    Arguments: rdd with body (cleaned) field for each doc in json format
    Returns: rdd with entitities field for each doc in json format
    """
    def process_partition(items):
        nlp = spacy.load()

        def process_document(data):
            document = json.loads(data)
            entities = {'person': [],
                        'location': [],
                        'organization': [],
                        'miscellaneous': []}
            lines = document['body'].replace('\\n', '\n').splitlines()
            for line in lines:
                for entity in nlp(line).ents:
                    if (entity.label_ == 'PER'):
                        entities['person'].append(entity.text)
                    elif (entity.label_ == 'LOC'):
                        entities['location'].append(entity.text)
                    elif (entity.label_ == 'LOC'):
                        entities['organization'].append(entity.text)
                    else:
                        entities['miscellaneous'].append(entity.text)
            document['entities'] = entities
            return json.dumps(document, ensure_ascii=False)

        for item in items:
            yield process_document(item)

    return rdd.mapPartitions(lambda x: process_partition(x))
