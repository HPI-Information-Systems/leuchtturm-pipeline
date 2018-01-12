"""This module processes emails."""

import json
import numpy as np
import email as email
import re
from dateutil import parser
import time
from talon.signature.bruteforce import extract_signature
from langdetect import detect
import spacy
# import emailbody.extractmailbody as body_extractor


def split_email(data):
    """Split email into parts and extract metadata.

    Arguments: data, a string in json format that has field raw.
    Returns: a string in json format with field parts.
    """
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

    header = re.compile(r'^[ ]*(?:From:.*[\n]?|To:.*[\n]?|Cc:.*[\n]?|Date:.*[\n]?|Sent:.*[\n]?|Subject:.*[\n]?)',
                        re.I | re.M)

    leading_spaces = re.compile(r'^(?:\n|\s|\t)+', re.M)

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

    def extract_metadata(part):
        """Extract metadata of one email part and return is at dictionary."""
        part = re.sub(leading_spaces, '', part)
        part_object = {}
        part_object['header'] = {}
        meta = email.message_from_string(part)
        for meta_element in meta.keys():
            part_object['header'][meta_element] = meta[meta_element]
        part_object['body'] = re.sub(header, ' ', part)

        return part_object

    document = json.loads(data)

    parts = detect_parts(document['raw'])

    part_objects = []
    for part in parts:
        part_objects.append(extract_metadata(part))

    document['parts'] = part_objects

    return json.dumps(document, ensure_ascii=False)


def extract_metadata(data):
    """Extract meta data of each email.

    Arguments: data, a string in json format that has field raw.
    Returns: a string in json format with field header.
    """
    def clean_subject_line(subject_line):
        return re.sub(r'(fw:|re:|aw:|fwd:) *', '', subject_line.lower())

    def unify_date(date_string):
        return time.mktime(parser.parse(date_string).timetuple())

    # this does not conver all senders, but has a very high accuracy for Enron data
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


def deduplicate_emails(data):
    """Deduplicate emails. Recognize emails by their header.

    Arguments: tba.
    Returns: tba.
    """
    # TODO: implement with mapreduce, since hashset might be inefficient and cause problems on very large datasets.

    return data


def extract_body(data):
    """Extract email body of each email.

    Arguments: data, a string in json format that has field raw.
    Returns: a string in json format with field body.
    """
    document = json.loads(data)
    mail_text = document['raw']
    # text_lines = mail_text.splitlines()

    # func_b = body_extractor.enron_two_zone_line_b_func
    # model = body_extractor.enron_two_zone_model

    # text_embedded = body_extractor.embed(text_lines, [func_b])
    # head_body_predictions = model.predict(np.array([text_embedded])).tolist()[0]

    # head_body_indicator = list(zip(text_lines, head_body_predictions))

    # body_text = ''
    # for i in range(0, len(head_body_indicator)):
    #     if int(head_body_indicator[i][1][0]) == int(1.0):
    #         body_text += head_body_indicator[i][0]

    document['body'] = mail_text

    return json.dumps(document, ensure_ascii=False)


def clean_entry(data):
    """Clean body of everything nlp algorithms might disturb.

    Arguments: data, a string in json format that has field body.
    Returns: a string in json format with field body.
    """
    special_chars = ['"', "!", "#", "$", "%", "&", "'", "§", "(", ")", "*", "+",
                     "-", ".", "/", ":", ";", "<", "=", ">", "?",
                     "@", "[", "\\", "]", "^", "_", "`", "{", "|", "}", "~", "\n", "\u000b", "\f"]

    rules = [r'\*\*\*\*\*\*\*\*\*\*\*(\n)?EDRM Enron Email(.)*(\n)?\*\*\*\*\*\*\*\*\*\*\*',
             r'----- Forwarded(.)*(From:(.)*|Subject:(.)*|To:(.)*|Sent:(.)*|Cc:(.)*|\n)*\n',
             r'-----Original Message-----(From:(.)*|Subject:(.)*|To:(.)*|Sent:(.)*|Cc:(.)*|\n)*\n',
             r'((From:)(.)*(\n)*)?To:(.)*(\n)*cc:(.)*(\n)*Subject:(.)*(\n)*',
             r'={76}(.|\n)*={76}']

    def clean_text(text):
        """Clean a string."""
        body_cleaned = text
        for rule in rules:
            body_cleaned = re.sub(rule, ' ', body_cleaned)

        body_cleaned, temp = extract_signature(body_cleaned)
        for sc in special_chars:
            body_cleaned = body_cleaned.replace(sc, ' ')

        return re.sub(r'(\n|\t| )+', ' ', body_cleaned)

    document = json.loads(data)

    document['body'] = clean_text(document['body'].replace('\\n', '\n'))

    for index, part in enumerate(document['parts']):
        document['parts'][index]['body'] = clean_text(document['parts'][index]['body'].replace('\\n', '\n'))

    return json.dumps(document, ensure_ascii=False)


def detect_language(data):
    """Detect language of each email.

    Arguments: data, a string in json format that has field body (cleaned).
    Returns: a string in json format with field lang.
    """
    document = json.loads(data)
    try:
        document['lang'] = detect(document['body'])
    except Exception:
        document['lang'] = 'unknown'
    return json.dumps(document, ensure_ascii=False)


nlp = spacy.load('en')


def extract_entities(data):
    """Extract entities of each email.

    Arguments: data, a string in json format that has field body (cleaned).
    Returns: a string in json format with field entities.
    """
    def make_entity(entity, entity_type, entity_count):
        """JSON Object defninition for entity."""
        return {
            "entity": entity,
            "entity_type": entity_type,
            "entity_count": entity_count
        }

    # TODO: drop counting and implement it in backend

    document = json.loads(data)
    doc = nlp(document['body'])
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
                make_entity(stripped_text, entity.label_, 1)
            )

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
