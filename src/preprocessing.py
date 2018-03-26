"""Pipes to preprocess emails, extract their meta-data, segmentation, ... for leuchtturm pipelines."""

from email import message_from_string
from email.utils import getaddresses, parsedate, parseaddr, unquote
from email.policy import default
import ujson as json
import re
from time import mktime
from datetime import datetime

from html2text import HTML2Text
from langdetect import detect
import textacy

from .common import Pipe


class EmailDecoding(Pipe):
    """Decode emails with multiparts, weid encodings and throw attachements away (for now).

    Return email with multiparts in format raw: header lines + body lines.
    Attachements may be considered in future.
    """

    def __init__(self, get_attachement_names=True):
        """Set conf."""
        super().__init__()
        self.get_attachement_names = get_attachement_names

    def decode_part(self, text, encoding=None):
        """Decode a mime part from e.g. base64 encoding."""
        if encoding is None:
            encoding = 'utf-8'

        return text.decode(encoding, 'ignore')

    def remove_html_tags(self, text):
        """Convert html to corresponding md."""
        h = HTML2Text()
        h.ignore_links = True
        h.ignore_emphasis = True
        h.ignore_images = True

        return h.handle(text)

    def get_body(self, message):
        """Return the text of the mime part representing the body and decode it. Take first if multiple."""
        body_text = ''
        for part in message.get_body(preferencelist=('html', 'plain')).walk():
            charset = part.get_content_charset()
            if part.get_content_type() == 'text/plain':
                text = part.get_payload(decode=True)
                body_text = self.decode_part(text, encoding=charset)
                break
            elif part.get_content_type() == 'text/html':
                text = part.get_payload(decode=True)
                body_text = self.remove_html_tags(self.decode_part(text, encoding=charset))
                break

        return body_text

    def get_main_header(self, message):
        """Return main header of email."""
        headers = ''
        for header in message.items():
            headers += header[0] + ': ' + header[1] + '\n'

        return headers + '\n\n'

    def get_attachement_names(self, message):
        """Return list of attachment file names."""
        file_names = []
        for part in message.walk():
            if part.is_attachment():
                file_names.append(part.get_filename())

        return file_names

    def run_on_document(self, document):
        """Get main body and extract attachement names on a leuchtturm doc."""
        doc = json.loads(document)
        try:
            message = message_from_string(doc['raw'], policy=default)
            doc['raw'] = self.get_main_header(message) + '\n\n' + self.get_body(message)
            doc['raw'] = textacy.preprocess.fix_bad_unicode(doc['raw'])
            if self.get_attachement_names:
                doc['attachments'] = self.get_attachement_names(message)
        except Exception:
            doc['raw'] = textacy.preprocess.fix_bad_unicode(doc['raw'])
            if self.get_attachement_names:
                doc['attachments'] = []

        return json.dumps(doc)

    def run(self, rdd):
        """Run task in spark context."""
        return rdd.map(lambda x: self.run_on_document(x))


class EmailSplitting(Pipe):
    """Split emails at their inline headers.

    Maximize information by adding inline coversations as separate documents.
    Use of this pipe is discouraged since correspondent deduplication is not yet implemented.
    """

    def __init__(self):
        """Set params if needed here."""
        super().__init__()

    def detect_parts(self, email):
        """Split email into its parts and return list of parts."""
        header = r'^(((subject:)|(from:)|(sent:)|(date:)|(to:)|(cc:))(\s.*\n)){4,}\s+'

        found_headers = re.finditer(header, email, re.MULTILINE | re.IGNORECASE | re.UNICODE)
        parts = [email]
        for found_header in found_headers:
            current_header = found_header.group()
            # skip first part since it was already added (main header is usually more complex)
            if not email.startswith(current_header):
                current_parts = email.split(current_header)
                parts.append(current_header + current_parts[1])

        return parts

    def run_on_document(self, raw_message):
        """Apply email splitting to a leuchtturm document. Return list of leuchtturm documents."""
        document = json.loads(raw_message)

        parts = self.detect_parts(document['raw'])

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

    def run(self, rdd):
        """Run pipe in spark context."""
        return rdd.flatMap(lambda x: self.run_on_document(x))


class HeaderParsing(Pipe):
    """Parse metadata of an email and split main header from body.

    Get sender, recipients, date, subject from emails in standard format.
    Get body from a mime multipart email.
    """

    def __init__(self, clean_subject=False, use_unix_time=False):
        """Set parsing rules."""
        super().__init__()
        self.clean_subject = clean_subject  # TODO is not implemented
        self.use_unix_time = use_unix_time

    def get_body(self, message):
        """Given a message object, return the text of the mime part representing the body and decode it."""
        return message.get_payload()

    def parse_correspondent(self, correspondent):
        """Given a tuple (name, email), return a correspondant dict."""
        parsed_correspondent = {'name': '', 'email': ''}
        if correspondent[0]:
            parsed_correspondent['name'] = unquote(correspondent[0])
        elif correspondent[1] and '@' not in correspondent[1]:
            parsed_correspondent['name'] = unquote(correspondent[1])
        if correspondent[1] and '@' in correspondent[1]:
            parsed_correspondent['email'] = unquote(correspondent[1]).lower()

        return parsed_correspondent

    def parse_date(self, date_string):
        """Normalize date from a string. If self.use_unix_time=True, return unix timestamp."""
        date = parsedate(date_string)
        date = mktime(date) if date is not None else 0

        return date if self.use_unix_time else datetime.fromtimestamp(int(date)).isoformat() + 'Z'

    def parse_subject(self, subject_string):
        """Clean subject line from RE:, AW: etc if self.clean_subject=True."""
        return subject_string

    def parse_header(self, message):
        """Given a message object, parse all relevant metadata and return them in a header dict."""
        header = {}
        header['sender'] = self.parse_correspondent(parseaddr(message.get('from', '')))
        header['recipients'] = []
        for recipient in getaddresses(message.get_all('to', []) +
                                      message.get_all('cc', []) +
                                      message.get_all('bcc', [])):
            if recipient[0] or recipient[1]:
                header['recipients'].append(self.parse_correspondent(recipient))
        header['subject'] = self.parse_subject(message.get('subject', ''))
        header['date'] = self.parse_date(message.get('date', '') + message.get('sent', ''))

        return header

    def run_on_document(self, raw_message):
        """Get body and header information for a leuchtturm document."""
        document = json.loads(raw_message)
        message = message_from_string(document['raw'])
        document['header'] = self.parse_header(message)
        document['body'] = self.get_body(message)

        return json.dumps(document)

    def run(self, rdd):
        """Run task in spark context."""
        return rdd.map(lambda x: self.run_on_document(x))


class LanguageDetection(Pipe):
    """Detect language of text.

    Given a json field name, this task can detect its language.
    Add 2 char language code indicating language in field lang.
    """

    def __init__(self, read_from='text_clean'):
        """Set lang detect params."""
        super().__init__()
        self.read_from = read_from

    def detect_lang(self, text):
        """Return 2 char lang code for a text."""
        try:
            return detect(text)
        except Exception:
            return 'xx'

    def run_on_document(self, raw_message):
        """Get language for a leuchtturm document."""
        document = json.loads(raw_message)
        document['lang'] = self.detect_lang(document[self.read_from])

        return json.dumps(document)

    def run(self, rdd):
        """Run task in spark context."""
        return rdd.map(lambda x: self.run_on_document(x))


class TextCleaning(Pipe):
    """Clean text from everythin that could cause problems to text mining algorithms.

    Convert to ascii, remove punctuation and overly used whitespace.
    Clean from inline headers and other email specific 'noise'.
    """

    def __init__(self, read_from='body', write_to='text_clean', readable=True):
        """Set params."""
        super().__init__()
        self.read_from = read_from
        self.write_to = write_to
        self.readable = readable

    def convert_to_ascii(self, text):
        """Replace unicode chars with their closest ascii char."""
        return textacy.preprocess.preprocess_text(text, fix_unicode=True, transliterate=True, no_contractions=True)

    def remove_header(self, text):
        """Remove email and enron specific noise from texts."""
        headers = [r'^(((subject:)|(from:)|(sent:)|(date:)|(to:)|(cc:))(\s.*\n)){3,}\s+',
                   r'----- forwarded.*((from:.*)|subject:(.)*|to:(.)*|sent:(.)*|cc:(.)*|\n)*\n',
                   r'-----\s?original message\s?-----',
                   r'(\*|=|-){40,}\s(.|\n)+(\*|=|-){40,}\s']

        for header in headers:
            text_clean = re.sub(header, '', text, re.MULTILINE | re.IGNORECASE | re.UNICODE)

        edrm_footer = ('***********\r\nEDRM Enron Email Data Set has been produced in EML, PST and NSF format by ZL '
                       'Technologies, Inc. This Data Set is licensed under a Creative Commons Attribution 3.0 United '
                       'States License <http://creativecommons.org/licenses/by/3.0/us/> . To provide attribution, '
                       'please cite to \"ZL Technologies, Inc. (http://www.zlti.com).\"\r\n***********')

        text_clean = text_clean.replace(edrm_footer, '')

        return text_clean

    def normalize_whitespace(self, text):
        """Replace 2+ spaces/newlines with 1 char."""
        return textacy.preprocess.normalize_whitespace(text)

    def remove_strict(self, text):
        """Remove everything(!) that could disturb tm tasks. Won't be readable afterwards."""
        return textacy.preprocess.preprocess_text(text, no_urls=True, no_emails=True, no_phone_numbers=True,
                                                  no_numbers=True, no_currency_symbols=True, no_punct=True)

    def run_on_document(self, raw_message):
        """Transform a document into clean text."""
        document = json.loads(raw_message)
        clean = document[self.read_from]
        for func in [self.remove_header, self.convert_to_ascii, self.normalize_whitespace]:
            clean = func(clean)
        clean = self.remove_strict(clean) if not self.readable else clean
        document[self.write_to] = clean

        return json.dumps(document)

    def run(self, rdd):
        """Run pipe in spark context."""
        return rdd.map(lambda x: self.run_on_document(x))
