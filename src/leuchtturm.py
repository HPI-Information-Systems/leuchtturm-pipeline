"""This module provides methods to processes emails and text."""

import json
import re
from email import message_from_string
from email.utils import getaddresses, parsedate, parseaddr, unquote
from time import mktime
from langdetect import detect
from gensim import corpora, models
from collections import defaultdict
import en_core_web_sm as spacy
import html2text
import pickle
from nltk.corpus import stopwords as nltksw
from nltk.stem.wordnet import WordNetLemmatizer
from string import punctuation, whitespace


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
        original_doc_id = document['doc_id']
        for index, part in enumerate(parts):
            obj = document
            obj['raw'] = part
            # if there are multiple parts, add an identifier to the original document id
            if len(parts) > 1:
                obj['doc_id'] = original_doc_id + '_part_' + str(index + 1) + '_of_' + str(len(parts))
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
        msg = message_from_string(document['raw'])

        def parse_correspondent_info(correspondent):
            parsed_correspondent = {'name': '', 'email': ''}
            if correspondent[0]:
                parsed_correspondent['name'] = unquote(correspondent[0])
            elif correspondent[1] and '@' not in correspondent[1]:
                parsed_correspondent['name'] = unquote(correspondent[1])
            if correspondent[1] and '@' in correspondent[1]:
                parsed_correspondent['email'] = unquote(correspondent[1]).lower()
            return parsed_correspondent

        header = {}
        sender = parseaddr(msg.get('from', ''))
        header['sender'] = parse_correspondent_info(sender)

        header['recipients'] = []
        for recipient in getaddresses(msg.get_all('to', []) + msg.get_all('cc', []) + msg.get_all('bcc', [])):
            if recipient[0] or [1]:
                header['recipients'].append(parse_correspondent_info(recipient))

        date = parsedate(msg.get('date', '') + msg.get('sent', ''))
        header['date'] = mktime(date) if (date is not None) else -1.0
        header['subject'] = msg.get('subject', '')
        document['header'] = header

        document['body'] = ''
        if msg.is_multipart():
            for payload in msg.walk():
                charset = payload.get_content_charset()
                if payload.get_content_type() == 'text/plain':
                    document['body'] = str(payload.get_payload(decode=True), str(charset), 'ignore')
                    break
                elif payload.get_content_type() == 'text/html':
                    document['body'] = html2text.html2text(str(payload.get_payload(decode=True),
                                                               str(charset), 'ignore'))
                    break
        else:
            charset = msg.get_content_charset()
            if msg.get_content_type() == 'text/plain':
                document['body'] = str(msg.get_payload(decode=True), str(charset), 'ignore')
            elif payload.get_content_type() == 'text/html':
                document['body'] = html2text.html2text(str(payload.msg(decode=True), str(charset), 'ignore'))

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
             r'----- forwarded.*((from:.*)|subject:(.)*|to:(.)*|sent:(.)*|cc:(.)*|\n)*\n',
             r'-----\s?original message\s?-----',
             r'(\*|=|-){40,}\s(.|\n)+(\*|=|-){40,}\s',
             r'\b\w{1,2}\b']

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


def train_topic_model(data):
    """Train topic model on cleaned bodies.

    Arguments: rdd with text_clean field for each doc in json format
    Returns: rdd with no further fields and stores a trained topic model
    """
    iterations = 1000
    num_topics = 100
    alpha = 50 / num_topics
    eta = 0.1
    raw_corpus = data

    stopwords = nltksw.words('english')

    lemma = WordNetLemmatizer()
    short_tokens = set()
    numbers = set()

    def clean(doc):
        tokens = [token for token in doc.lower().split()]
        punc_free = [token.strip(punctuation) for token in tokens]
        empty_string_free = [token for token in punc_free if token]
        stopword_free = [word for word in empty_string_free if word not in stopwords]
        short_token_free = [word if len(word) > 2 else short_tokens.add(word) for word in stopword_free]
        empty_string_free2 = [token for token in short_token_free if token]
        numerics_free = []
        for token in empty_string_free2:
            if [char for char in token if not (char.isdigit() or char in punctuation)]:
                numerics_free.append(token)
            else:
                numerics_free.append('lt_number')
                numbers.add(token)
        lemmatized = [lemma.lemmatize(word) for word in numerics_free]
        return lemmatized

    docs = [clean(doc) for doc in raw_corpus]

    word_doc_appearances = defaultdict(set)
    for i, doc in enumerate(docs):
        for token in doc:
            word_doc_appearances[token].add(i)

    high_freq_tokens = set()
    low_freq_tokens = set()

    MIN_FREQ = 3
    MAX_PERCENTAGE = 0.05
    max_freq = MAX_PERCENTAGE * len(docs)

    def filter_by_freq(doc):
        filtered_doc = []
        for token in doc:
            if token == 'lt_number':
                filtered_doc.append(token)
            elif len(word_doc_appearances[token]) < MIN_FREQ:
                low_freq_tokens.add(token)
            elif len(word_doc_appearances[token]) > max_freq:
                high_freq_tokens.add(token)
            else:
                filtered_doc.append(token)
        return filtered_doc

    docs = [filter_by_freq(doc) for doc in docs]

    docs = [doc for doc in docs if doc]

    processed_corpus = docs

    dictionary = corpora.Dictionary(processed_corpus)
    with open('./models/pickled_lda_dictionary.p', 'wb') as pfile:
        pickle.dump(dictionary, pfile)

    bow_corpus = [dictionary.doc2bow(text) for text in processed_corpus]

    lda = models.ldamodel.LdaModel(bow_corpus, num_topics=num_topics, iterations=iterations, eta=eta, alpha=alpha)
    with open('./models/pickled_lda_model.p', 'wb') as pfile:
        pickle.dump(lda, pfile)


def extract_topics(rdd):
    """Extract topics from cleaned email bodies.

    Arguments: rdd with text_clean field for each doc in json format
    Returns: rdd with a topics field for each doc in json format
    """
    def process_partition(items):
        with open('./models/pickled_lda_model.p', mode='rb') as pfile:
            lda = pickle.load(pfile)

        with open('./models/pickled_lda_dictionary.p', mode='rb') as pfile:
            dictionary = pickle.load(pfile)

        def process_document(data):
            document = json.loads(data)

            bow = dictionary.doc2bow(document['text_clean'].split())

            topic_terms = []
            for topic in lda.get_document_topics(bow):
                terms = map(lambda xy: (dictionary[xy[0]], xy[1]), lda.get_topic_terms(topic[0], topn=10))
                topic_terms.append(str((str(topic[1]), (list(terms)))))

            document['topics'] = str(topic_terms)

            return json.dumps(document)

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
                for entity in filter(lambda x: x.text.strip(whitespace) != '', nlp(line).ents):
                    if (entity.label_ == 'PERSON'):
                        entities['person'].append(entity.text.strip(whitespace))
                    elif (entity.label_ == 'LOC' or entity.label_ == 'GPE' or entity.label_ == 'FAC'):
                        entities['location'].append(entity.text.strip(whitespace))
                    elif (entity.label_ == 'ORG' or entity.label_ == 'NORP'):
                        entities['organization'].append(entity.text.strip(whitespace))
                    elif (entity.label_ == 'PRODUCT' or entity.label_ == 'EVENT' or entity.label_ == 'WORK_OF_ART'):
                        entities['miscellaneous'].append(entity.text.strip(whitespace))

            document['entities'] = {'person': list(set(entities['person'])),
                                    'location': list(set(entities['location'])),
                                    'organization': list(set(entities['organization'])),
                                    'miscellaneous': list(set(entities['miscellaneous']))}

            return json.dumps(document)

        for item in items:
            yield process_document(item)

    return rdd.mapPartitions(lambda x: process_partition(x))
