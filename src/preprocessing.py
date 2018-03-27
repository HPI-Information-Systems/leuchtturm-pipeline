"""Pipes to preprocess emails, extract their meta-data, segmentation, ... for leuchtturm pipelines."""

from email import message_from_string
from email.utils import getaddresses, parsedate, parseaddr, unquote
from email.policy import default
import ujson as json
import re
from string import whitespace
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
        keys = re.compile(r'(from)|(x-from)|(to)|(x-to)|(cc)|(x-cc)|(bcc)|(x-bcc)|(subject)|(date)',
                          re.UNICODE | re.IGNORECASE)

        # filter headers for following splitting task
        filtered_headers = [header for header in message.items() if keys.match(header[0])]
        headers = ''
        for header in filtered_headers:
            headers += header[0] + ': ' + header[1] + '\n'

        return headers + '\n\n'

    def get_attachement_names(self, message):
        """Return list of attachment file names."""
        enron_file = message.get('x-filename', '')
        file_names = [] if not enron_file else [enron_file]
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

    header = re.compile(r'((((\t)*)(((-+).*\n(.*-+))\n{0,4}(.*\n){0,3})?(.+\n))|(\S.*\n){0,2})((\t|>)* ?((\n*subject:)|(from:)|(reply-to:)|(sent by:)|(sent:)|(date:)|(to:)|(cc:))(\s.*\n)(.*(@|;|and).*\n)*){3,}', re.MULTILINE | re.IGNORECASE | re.UNICODE)  # NOQA

    def __init__(self):
        """Set params if needed here."""
        super().__init__()

    def detect_parts(self, email):
        """Split email into its parts and return list of parts."""
        found_headers = list(EmailSplitting.header.finditer(email))
        headers = [head.group() for head in found_headers]
        parts = []

        # when no headers are found is the entire unsplit mail to be added
        if not headers:
            parts.append(email)
        for index, found_header in enumerate(headers):
            current_header = found_header
            next_header = ''
            # determine the next header if the current one is not the last already
            if index < len(headers) - 1:
                next_header = headers[index + 1]

            current_parts = email.split(current_header)
            if next_header:
                parts.append((current_header + current_parts[1].split(next_header)[0], current_header))
            else:
                parts.append((current_header + current_parts[1], current_header))

        return parts

    def run_on_document(self, raw_message):
        """Apply email splitting to a leuchtturm document. Return list of leuchtturm documents."""
        document = json.loads(raw_message)

        parts = self.detect_parts(document['raw'])

        part_docs = []
        splitted_emails = []

        original_doc_id = document['doc_id']
        for index, (part, header) in enumerate(parts):
            obj = document
            obj['header'] = header if part.startswith(header) else None
            obj['body'] = part.replace(header, '')

            # if there are multiple parts, add an identifier to the original document id
            if len(parts) > 1:
                obj['doc_id'] = original_doc_id + '_part_' + str(index + 1) + '_of_' + str(len(parts))

            part_docs.append(dict(obj))

        for index, part in enumerate(part_docs):
            obj = part
            obj['successors'] = [part_docs[index - 1]['doc_id'] if not index == 0 else None]
            obj["predecessor"] = part_docs[index + 1]['doc_id'] if not index == len(part_docs) - 1 else None
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

    def prepare_string(self, text):
        """Remove whitespace, newlines and other noise."""
        text = text.replace('----- Original Message -----', '')
        text = re.sub(r'^(\s|>)+', '', text, re.MULTILINE)  # remove leading >
        text = re.sub(r'\s+', ' ', text)  # normalize whitespace
        text = text.strip(whitespace)

        return text

    def transform_header_string(self, header_string):
        """Split a string that is likely a header into its fields."""
        header_string = self.prepare_string(header_string)

        header_fields = re.split(r'\s(?=[a-zA-Z-]+:\s)', header_string)  # split different headers
        for index, header_field in enumerate(header_fields):
            header_fields[index] = header_field.lower().split(': ', 1)  # make key value pairs out of a header

        return header_fields

    def get_header_value(self, transformed_header, field):
        """Get value from a transformed header list."""
        field = [header for header in transformed_header if header[0] == field]

        return field[0][1] if field else ''

    def parse_correspondent(self, correspondent):
        """Given a tuple (name, email), return a correspondant dict."""
        return correspondent

    def parse_recipients(self, field_string, type='to'):
        """Parse a list (in string format) to correspondents."""
        return field_string

    def parse_date(self, date_string):
        """Normalize date from a string. If self.use_unix_time=True, return unix timestamp."""
        # date = parsedate(date_string)
        # date = mktime(date) if date is not None else 0

        # return date if self.use_unix_time else datetime.fromtimestamp(int(date)).isoformat() + 'Z'
        return date_string

    def parse_subject(self, subject_string):
        """Clean subject line from RE:, AW: etc if self.clean_subject=True."""
        return subject_string

    def parse_header(self, header_string):
        """Given a message object, parse all relevant metadata and return them in a header dict."""
        header = {'sender': '',
                  'recipients': [],
                  'date': '',
                  'subject': ''}

        headers = self.transform_header_string(header_string)

        if self.get_header_value(headers, 'from'):
            header['sender'] = self.parse_correspondent(self.get_header_value(headers, 'from'))
        else:
            sender = re.sub(r'\d{2}/\d{2}/\d{4}\s\d{2}:\d{2}(:\d{2})?\s(PM|AM)', '', headers[0],
                            re.UNICODE | re.IGNORECASE)  # remove date
            header['sender'] = self.parse_correspondent(sender)

        if self.get_header_value(headers, 'to'):
            header['recipients'].append(self.parse_recipients(self.get_header_value(headers, 'to'), type='to'))
        if self.get_header_value(headers, 'cc'):
            header['recipients'].append(self.parse_recipients(self.get_header_value(headers, 'cc'), type='cc'))
        if self.get_header_value(headers, 'bcc'):
            header['recipients'].append(self.parse_recipients(self.get_header_value(headers, 'to'), type='bcc'))

        if self.get_header_value(headers, 'date'):
            header['date'] = self.parse_date(self.get_header_value(headers, 'date'))
        elif self.get_header_value(headers, 'sent'):
            header['date'] = self.parse_date(self.get_header_value(headers, 'sent'))
        else:
            header['date'] = re.search(r'\d{2}/\d{2}/\d{4}\s\d{2}:\d{2}(:\d{2})?\s(PM|AM)', headers[0]).group()

        if self.get_header_value(headers, 'subject'):
            header['subject'] = self.parse_subject(self.get_header_value(headers, 'subject'))

        return header

    def run_on_document(self, raw_message):
        """Get body and header information for a leuchtturm document."""
        document = json.loads(raw_message)
        document['header'] = self.parse_header(document['header'])

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
