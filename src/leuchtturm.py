"""This module provides methods to processes emails and text."""


import json
import re
import email
from email.utils import getaddresses, parsedate, parseaddr, unquote
from time import mktime
from langdetect import detect
import en_core_web_sm as spacy
from gensim import corpora
from gensim import models
from nltk.corpus import stopwords 
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import wordnet
from string import punctuation
from hdfs import Client
import pickle



def split_emails(rdd):
    """Split email into parts and extract metadata.

    Arguments: rdd with raw, doc_id field for each doc in json format
    Returns: rdd with body field for each doc in json format splitted by parts of conversation
    """
    def split_document(data):
        header = r'^((subject: .*\n)|(from: .*\n)|(sent: .*\n)|(date: .*\n)|(to: .*\n)|(cc: .*\n)){4,}\n$\n'

        def detect_parts(email):
            """Split email into parts and return list of parts."""
            found_headers = re.finditer(header, email, re.MULTILINE | re.IGNORECASE)
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
        document = json.loads(data)
        msg = email.message_from_string(document['body'])

        header = {}
        header['sender'] = {'name': unquote(parseaddr(msg.get('from', ''))[0]),
                            'email': unquote(parseaddr(msg.get('from', ''))[1].lower())}
        header['recipients'] = []
        for recipient in getaddresses(msg.get_all('to', []) + msg.get_all('cc', []) + msg.get_all('bcc', [])):
            header['recipients'].append({'name': unquote(recipient[0]),
                                         'email': unquote(recipient[1].lower())})
        date = parsedate(msg.get('date', '') + msg.get('sent', ''))
        header['date'] = mktime(date) if (len(date) == 9) else 0.0
        header['subject'] = msg.get('subject', '')

        document['header'] = header

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
                                     data_norm['header']['date'],
                                     data_norm['header']['subject']],
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


def extract_topics(rdd):
    """Extract topics from cleaned email bodies.

    Arguments: rdd with body field for each doc in json format
    Returns: rdd with a cleaned body field for each doc in json format
    """

    hdfs_client = Client('http://172.18.20.109:50070')

    def process_partition(items):
        
        with hdfs_client.read_file("models/pickled_lda_model.p") as pfile:
            lda = pickle.loads(pfile)
    
        with hdfs_client.read_file("models/pickled_lda_dictionary.p") as pfile:
            dictionary = pickle.loads(pfile)        

        def process_document(data):
            document = json.loads(data)

            # CLEAN DOCUMENTS
            def split_into_tokens(doc_string):
                return [token for token in doc_string.lower().split()]

            def strip_punctuation(doc):
                return [token.strip(punctuation) for token in doc]

            def rm_empty_tokens(doc):
                # removes all tokens == ''
                return [token for token in doc if token]

            def rm_stopwords(doc):
                return [token for token in doc if token not in stopwords]

            def rm_numeric_tokens(doc):
                result_doc = []
                for token in doc:
                    if [char for char in token if not (char.isdigit() or char in punctuation)]:
                        result_doc.append(token)
                    else:
                        result_doc.append('lt_number')
                        numeric_tokens.add(token)
                return result_doc

            def rm_short_tokens(doc, min_len=3):
                result_doc = []
                for token in doc:
                    if len(token) >= min_len:
                        result_doc.append(token)
                    else:
                        short_tokens.add(token)
                return result_doc

            def lemmatize_tokens(doc):
                result_doc = []
                for token in doc:
                    lemmatizations = []
                    for part_of_speech in [wordnet.NOUN, wordnet.VERB, wordnet.ADJ, wordnet.ADV]:
                        lemmatization = lemma.lemmatize(token, part_of_speech)
                        if lemmatization != token:
                            lemmatizations.append(lemmatization)
                    if lemmatizations:
                        shortest_lemmatization = min(lemmatizations, key=len)
                        result_doc.append(shortest_lemmatization)
                    else:
                        result_doc.append(token)
                return result_doc

            def clean(doc):
                result_doc = split_into_tokens(doc)
                result_doc = strip_punctuation(result_doc)
                result_doc = rm_empty_tokens(result_doc)
                result_doc = rm_stopwords(result_doc)
                result_doc = rm_numeric_tokens(result_doc)
                result_doc = rm_short_tokens(result_doc)
                result_doc = lemmatize_tokens(result_doc)
                if verbose_log_enabled:
                    print('before:', doc)
                    print('after: ', result_doc, '\n')
                return result_doc

            document = clean(document)

            # TRANSFORM THEM TO BOW
            bow = dictionary.doc2bow(document)

            get_document_topics(bow, minimum_probability=None, minimum_phi_value=None, per_word_topics=False)¶

            document['topics'] = topics
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
                    if (entity.label_ == 'PERSON'):
                        entities['person'].append(entity.text)
                    elif (entity.label_ == 'LOC' or entity.label_ == 'GPE' or entity.label_ == 'FAC'):
                        entities['location'].append(entity.text)
                    elif (entity.label_ == 'ORG' or entity.label_ == 'NORP'):
                        entities['organization'].append(entity.text)
                    elif (entity.label_ == 'PRODUCT' or entity.label_ == 'EVENT' or entity.label_ == 'WORK_OF_ART'):
                        entities['miscellaneous'].append(entity.text)
            document['entities'] = entities
            return json.dumps(document, ensure_ascii=False)

        for item in items:
            yield process_document(item)

    return rdd.mapPartitions(lambda x: process_partition(x))
