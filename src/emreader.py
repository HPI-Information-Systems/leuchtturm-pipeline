from email import message_from_file
import os
import re
import ast
import html2text
import mimetypes
import codecs
import sys
from pprint import pprint

# https://stackoverflow.com/questions/31392361/how-to-read-eml-file-in-python?utm_medium=organic&utm_source=google_rich_qa&utm_campaign=google_rich_qa
# http://recherche-redaktion.de/tutorium/sonderz_kauderw.htm

# Path to directory where attachments will be stored:
path = "../../private_mails/input"


def parse_part(part, full_parse=True):
    # decode the string if it was encoded
    try:
        txt = part.get_payload(decode=True)
    except:
        txt = part.get_payload(decode=False)

    try:
        txt = txt.decode('utf-8')
    except UnicodeDecodeError:
        txt = txt.decode('iso-8859-1')
    except:
        txt = str(txt)

    if not full_parse:
        return txt

    # sometimes the string contains unicode to ascii artifacts, clean that up!
    try:
        txt = re.sub(r'=([A-Z0-9]{2})', lambda x: ast.literal_eval('"\\x' + x.group(1).lower() + '"'), txt)
    except SyntaxError:
        pass

    # sometimes the string was actually encoded in iso-8859-1,
    # but the encoding is declared to be utf-8
    try:
        if part.get_content_charset() == 'iso-8859-1':
            txt = codecs.decode(txt, 'raw_unicode_escape').encode('iso-8859-1').decode()
        else:
            txt = codecs.decode(txt, 'unicode_escape').encode('iso-8859-1').decode()
    except UnicodeDecodeError:
        pass

    try:
        if part.get_content_type() == 'text/html':
            return html2text.html2text(txt)
    except:
        pass

    # else assume 'text/plain':
    return txt


def extract_parts(message):
    parsed = {
        'plain': None,
        'html': None,
        'html_raw': None,
        'plain_raw': None,
        'attachments': []
    }

    for part in message.walk():
        # multipart/* are just containers
        maintype = part.get_content_maintype()
        if maintype == 'multipart':
            continue

        content_disposition = part.get_content_disposition()
        ctype = part.get_content_type()

        # email body
        if content_disposition is None:
            key = 'html' if ctype == 'text/html' else 'plain'
            parsed[key] = parse_part(part, full_parse=True)
            parsed[key + '_raw'] = parse_part(part, full_parse=False)

        # email attachment
        elif content_disposition == 'attachment':
            parsed['attachments'].append({
                'filename': part.get_filename(),
                'maintype': maintype,
                'content_type': ctype
            })

        # can there be something else?
        # TODO check if there are other content_dispositions
        else:
            pass

    return parsed


for file in os.listdir(path):
    # if file.endswith('HPI-1.eml'):
    if file.endswith('.eml'):
        print(file)
        fn = os.path.abspath(os.path.join(path, file))
        counter = 0
        print(fn)
        with open(fn, 'r') as f:
            msg = message_from_file(f)

            extracted = extract_parts(msg)
            extracted['header'] = dict(msg.items())
            extracted['header']['content_type'] = msg.get_content_type()

            pprint(extracted)
            break
