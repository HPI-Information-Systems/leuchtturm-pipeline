# -*- coding: utf-8 -*-

"""This module provides methods to processes emails and text."""

import json
import re
import email
from email.utils import getaddresses, parsedate, parseaddr, unquote
from time import mktime
from langdetect import detect
import en_core_web_sm as spacy
from hdfs import Client
import pickle
from settings import hdfs_client_url, path_lda_model, path_lda_dict


def split_emails(rdd):
    """Split email into parts and extract metadata.

    Arguments: rdd with raw, doc_id field for each doc in json format
    Returns: rdd with body field for each doc in json format splitted by parts of conversation
    """
    def split_document(data):
        header = r'^(((subject:)|(from:)|(sent:)|(date:)|(to:)|(cc:))(\s.*\n)){4,}\s+'

        def detect_parts(email):
            """Split email into parts and return list of parts."""
            found_headers = re.finditer(header, email, re.MULTILINE | re.IGNORECASE | re.UNICODE)
            parts = [email]
            for found_header in found_headers:
                current_header = found_header.group()
                if not email.startswith(current_header):
                    current_parts = email.split(current_header)
                    parts.append(current_header + current_parts[1])

            return parts

        document = json.loads(data)

        parts = detect_parts(document['raw'])

        splitted_emails = []
        for part in parts:
            obj = document
            obj['raw'] = part
            splitted_emails.append(json.dumps(obj))

        return splitted_emails

    return rdd.flatMap(lambda x: split_document(x))


def extract_metadata(rdd):
    """Extract meta data of each email.

    Arguments: rdd with body field for each doc in json format
    Returns: rdd with header field for each doc in json format
    """
    def add_metadata(data):
        document = json.loads(data)
        msg = email.message_from_string(document['raw'])

        header = {}
        header['sender'] = {'name': unquote(parseaddr(msg.get('from', ''))[0]),
                            'email': unquote(parseaddr(msg.get('from', ''))[1].lower())}
        header['recipients'] = []
        for recipient in getaddresses(msg.get_all('to', []) + msg.get_all('cc', []) + msg.get_all('bcc', [])):
            if recipient[0] or recipient[1]:
                header['recipients'].append({'name': unquote(recipient[0]),
                                             'email': unquote(recipient[1].lower())})
        date = parsedate(msg.get('date', '') + msg.get('sent', ''))
        header['date'] = mktime(date) if (date is not None) else -1.0
        header['subject'] = msg.get('subject', '')
        document['header'] = header

        document['body'] = ''
        if msg.is_multipart():
            for payload in msg.get_payload():
                document['body'] += payload.get_payload()
        else:
            document['body'] = msg.get_payload()

        return json.dumps(document)

    return rdd.map(lambda x: add_metadata(x))


def deduplicate_emails(rdd):
    """Deduplicate emails. Recognize emails by their header.

    Arguments: rdd with raw, header field for each doc in json format
    Returns: rdd that no longer contains duplicates as defined in select_email()
    """
    def convert_to_tupel(data):
        data_norm = json.loads(data)
        splitting_keys = json.dumps([data_norm['header']['sender']['email'],
                                     data_norm['header']['date'],
                                     data_norm['header']['subject']])

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
    special_chars = ['"', "!", "#", "$", "%", "&", "'", "§", "(", ")", "*", ",",
                     "-", "/", ":", ";", "<", "=", ">", "?", "\x97", "+", "\n", "\t", "\r",
                     "@", "[", "\\", "]", "^", "_", "`", "{", "|", "}", "~", "\u000b", "\f"]

    rules = [r'<[^>].+>',
             r'^(((subject:)|(from:)|(sent:)|(date:)|(to:)|(cc:))(\s.*\n)){3,}\s+',
             r'----- forwarded.*((from:.*)|fubject:(.)*|to:(.)*|sent:(.)*|cc:(.)*|\n)*\n',
             r'-----\s?original message\s?-----',
             r'(\*|=|-){40,}\s(.|\n)+(\*|=|-){40,}\s']

    edrm_footer = ('***********\r\nEDRM Enron Email Data Set has been produced in EML, PST and NSF format by ZL '
                   'Technologies, Inc. This Data Set is licensed under a Creative Commons Attribution 3.0 United '
                   'States License <http://creativecommons.org/licenses/by/3.0/us/> . To provide attribution, '
                   'please cite to \"ZL Technologies, Inc. (http://www.zlti.com).\"\r\n***********')

    def clean_document(data):
        document = json.loads(data)

        text_clean = document['header']['subject'] + '. ' + document['body']
        text_clean = text_clean.replace(edrm_footer, '')
        document['body'] = document['body'].replace(edrm_footer, '')
        for rule in rules:
            text_clean = re.sub(rule, ' ', text_clean, re.MULTILINE | re.IGNORECASE | re.UNICODE)

        # remove non ascii chars and special chars
        text_clean = ''.join([char if ord(char) < 128 else ' ' for char in text_clean])
        for sc in special_chars:
            text_clean = text_clean.replace(sc, ' ')
        # clean whitespace
        text_clean = re.sub(r'^\s+|\s+$|\s+(?=\s)', '', text_clean, re.MULTILINE | re.IGNORECASE | re.UNICODE)
        document['text_clean'] = text_clean

        return json.dumps(document)

    return rdd.map(lambda x: clean_document(x))


def extract_topics(rdd):
    """Extract topics from cleaned email bodies.

    Arguments: rdd with text_clean field for each doc in json format
    Returns: rdd with a topics field for each doc in json format
    """
    hdfs_client = Client(hdfs_client_url)

    def process_partition(items):
        with hdfs_client.read(path_lda_model, encoding='utf-8') as pfile:
            lda = pickle.loads(pfile.data)

        with hdfs_client.read(path_lda_dict, encoding='utf-8') as pfile:
            dictionary = pickle.loads(pfile.data)

        def process_document(data):
            document = json.loads(data)

            bow = dictionary.doc2bow(document['body'].split())

            topic_terms = []

            topics = lda.get_document_topics(bow)

            for topic in topics:
                terms = map(lambda xy: (dictionary[xy[0]], xy[1]), lda.get_topic_terms(topic[0], topn=10))
                topic_terms.append(str((str(topic[1]), (list(terms)))))

            document['topics'] = str(topic_terms)

            return json.dumps(document, ensure_ascii=False)

        for item in items:
            yield process_document(item)

    return rdd.mapPartitions(lambda x: process_partition(x))


def detect_languages(rdd):
    """Detect language of each email.

    Arguments: rdd with body (cleaned) field for each doc in json format
    Returns: rdd with lang field for each doc in json format ('xx' if unknown)
    """
    def detect_email_lang(data):
        document = json.loads(data)
        try:
            document['lang'] = detect(document['text_clean'])
        except Exception:
            document['lang'] = 'xx'
        return json.dumps(document)

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

            lines = [document['text_clean'][i: i + 1000] for i in range(0, len(document['text_clean']), 1000)]
            for line in lines:
                for entity in filter(lambda x: x.text != ' ', nlp(line).ents):
                    if (entity.label_ == 'PERSON'):
                        entities['person'].append(entity.text)
                    elif (entity.label_ == 'LOC' or entity.label_ == 'GPE' or entity.label_ == 'FAC'):
                        entities['location'].append(entity.text)
                    elif (entity.label_ == 'ORG' or entity.label_ == 'NORP'):
                        entities['organization'].append(entity.text)
                    elif (entity.label_ == 'PRODUCT' or entity.label_ == 'EVENT' or entity.label_ == 'WORK_OF_ART'):
                        entities['miscellaneous'].append(entity.text)

            document['entities'] = {'person': list(set(entities['person'])),
                                    'location': list(set(entities['location'])),
                                    'organization': list(set(entities['organization'])),
                                    'miscellaneous': list(set(entities['miscellaneous']))}

            return json.dumps(document)

        for item in items:
            yield process_document(item)

    return rdd.mapPartitions(lambda x: process_partition(x))
