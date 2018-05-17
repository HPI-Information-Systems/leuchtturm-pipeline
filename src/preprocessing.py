"""Pipes to preprocess emails, extract their meta-data, segmentation, ... for leuchtturm pipelines."""

import ast
from email import message_from_string
from email.utils import unquote
from email.header import make_header, decode_header
import codecs
import datetime
import ujson as json
import re
from string import whitespace

from html2text import HTML2Text
from langdetect import detect
import textacy
import dateparser

from .common import Pipe


class EmailDecoding(Pipe):
    """Decode emails with multiparts, weid encodings and throw attachements away (for now).

    Return email with multiparts in format raw: header lines + body lines.
    Attachements may be considered in future.
    """

    def __init__(self, conf, get_attachment_names=True, split_header_body=False):
        """Set conf."""
        super().__init__(conf)
        self.get_attachment_names = get_attachment_names
        self.split_header_body = split_header_body

    def decode_part(self, text, encoding=None):
        """Decode a mime part from e.g. base64 encoding."""
        if encoding is None:
            encoding = 'utf-8'

        try:
            text = text.decode(encoding, 'replace')
        except LookupError:
            try:
                text = text.decode('utf-8', 'strict')
            except UnicodeDecodeError:
                text = text.decode('iso-8859-1')
            except Exception:
                text = str(text)

        # sometimes the string contains unicode to ascii artifacts, clean that up!
        try:
            text = re.sub(r'=([A-Z0-9]{2})', lambda x: ast.literal_eval('"\\x' + x.group(1).lower() + '"'), text)
        except SyntaxError:
            pass

        # sometimes the string was actually encoded in iso-8859-1,
        # but the encoding is declared to be utf-8
        try:
            if encoding == 'iso-8859-1':
                text = codecs.decode(text, 'raw_unicode_escape').encode('iso-8859-1', errors='replace').decode()
            else:
                text = codecs.decode(text, 'unicode_escape').encode('iso-8859-1', errors='replace').decode()
        except UnicodeDecodeError:
            pass

        # else assume 'text/plain':
        return text

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
        for part in message.walk():
            charset = part.get_content_charset()
            if part.get_content_type() == 'text/plain' or part.get_content_type() == 'text/html':
                text = part.get_payload(decode=True)
                body_text = self.decode_part(text, encoding=charset)
                if re.search(r'<[^>]+>', body_text) and re.search(r'</[^>]+>', body_text):
                    body_text = self.remove_html_tags(body_text)
                break

        if not body_text:
            body_text = '[Empty message]'

        return body_text

    def get_main_header(self, message):
        """Return main header of email."""
        keys = re.compile(r'(from)|((reply-)?to)|(b?cc)|(subject)|(date)|(sent(-by)?)', re.IGNORECASE)

        # filter headers for following splitting task
        filtered_headers = [header for header in message.items() if keys.match(header[0])]
        headers = ''
        for header in filtered_headers:
            try:
                header_dec = str(make_header(decode_header(header[1])))
            except UnicodeDecodeError:
                header_dec = header[1]
            headers += header[0] + ': ' + header_dec + '\n'

        return headers

    def get_attachments(self, message):
        """Return list of attachment file names."""
        file_names = []
        for part in message.walk():
            if part.get_filename():
                file_names.append(part.get_filename())

        return file_names

    def run_on_document(self, document):
        """Get main body and extract attachement names on a leuchtturm doc."""
        doc = json.loads(document)
        message = message_from_string(doc['raw'])

        minimal_header = textacy.preprocess.fix_bad_unicode(self.get_main_header(message))
        body = textacy.preprocess.fix_bad_unicode(self.get_body(message))

        doc['raw'] = minimal_header + '\n\n' + body
        if self.split_header_body:
            doc['header'] = minimal_header
            doc['body'] = body
        if self.get_attachment_names:
            doc['attachments'] = self.get_attachments(message)

        return json.dumps(doc, ensure_ascii=False)

    def run(self, rdd):
        """Run task in spark context."""
        return rdd.map(lambda x: self.run_on_document(x))


class EmailSplitting(Pipe):
    """Split emails at their inline headers.

    Maximize information by adding inline coversations as separate documents.
    Pointers to context emails are being added.
    """

    forwarded_by_heuristic = r'(.*-{3}.*Forwarded by((\n|.)*?)Subject:.*)'
    begin_forwarded_message_heuristic = r'(.*Begin forwarded message:((\n|.)*?)To:.*)'
    original_message_heuristic = r'(.*-{3}.*Original Message((\n|.)*?)Subject:.*)'
    reply_seperator_heuristic = r'(.*_{3}.*Reply Separator((\n|.)*?)Date.*)'
    date_to_subject_heuristic = r'(.*\n.*(on )?\d{2}\/\d{2}\/\d{2,4}\s\d{2}:\d{2}(:\d{2})?\s?(AM|PM|am|pm)?.*\n.*(\n.*)?To: (\n|.)*?Subject: .*)'  # NOQA
    from_to_subject_heuristic = r'(.*From:((\n|.)*?)Subject:.*)'

    header_regex = re.compile('(%s|%s|%s|%s|%s|%s)' % (
        forwarded_by_heuristic,
        begin_forwarded_message_heuristic,
        original_message_heuristic,
        reply_seperator_heuristic,
        date_to_subject_heuristic,
        from_to_subject_heuristic
    ))

    def __init__(self, conf, keep_thread_connected=False, use_quagga=False):
        """Set params if needed here."""
        super().__init__(conf)
        self.keep_thread_connected = keep_thread_connected
        self.use_quagga = use_quagga

    def detect_parts_quagga(self, email):
        """Split email into its parts using quagga. This is experimental."""
        from .libs.quagga import detect_parts

        return detect_parts(email)

    def detect_parts(self, email):
        """Split email into its parts and return list of parts."""
        original_header, original_body = email.split('\n\n', 1)

        found_headers = list(EmailSplitting.header_regex.finditer(original_body))
        headers = [header.group() for header in found_headers]
        headers = [original_header] + headers

        remaining_email = email
        parts = []

        # when no headers are found is the entire unsplit mail to be added
        if not headers:
            parts.append(remaining_email)
        for index, found_header in enumerate(headers):
            current_header = found_header
            next_header = ''
            # determine the next header if the current one is not the last already
            if index < len(headers) - 1:
                next_header = headers[index + 1]

            current_parts = remaining_email.split(current_header, 1)

            if next_header:
                parts.append((current_header, current_parts[1].split(next_header)[0]))
            else:
                parts.append((current_header, current_parts[1]))

        return parts

    def run_on_document(self, raw_message):
        """Apply email splitting to a leuchtturm document. Return list of leuchtturm documents."""
        document = json.loads(raw_message)

        parts = []
        if self.use_quagga:
            parts = self.detect_parts_quagga(document['raw'])
        else:
            parts = self.detect_parts(document['raw'])

        part_docs = []
        splitted_emails = []

        original_doc_id = document['doc_id']
        for index, (header, body) in enumerate(parts):
            obj = document
            obj['header'] = header
            obj['body'] = body.strip(whitespace)

            if len(parts) > 1:  # if there are multiple parts, add an identifier to the original document id
                obj['doc_id'] = original_doc_id + '_part_' + str(index + 1) + '_of_' + str(len(parts))

            part_docs.append(obj.copy())

        for index, part in enumerate(part_docs):
            part['successor'] = part_docs[index - 1]['doc_id'] if not index == 0 else None
            part['predecessor'] = part_docs[index + 1]['doc_id'] if not index == len(part_docs) - 1 else None
            splitted_emails.append(part.copy())

        if self.keep_thread_connected:
            document['parts'] = splitted_emails
            return json.dumps(document, ensure_ascii=False)
        else:
            return [json.dumps(email, ensure_ascii=False) for email in splitted_emails]

    def run(self, rdd):
        """Run pipe in spark context."""
        if self.keep_thread_connected:
            return rdd.map(lambda x: self.run_on_document(x))

        else:
            return rdd.flatMap(lambda x: self.run_on_document(x))


class HeaderParsing(Pipe):
    """Parse metadata of an email and split main header from body.

    Given a splitted document (header/body).
    Get sender, recipients, date, subject from emails even with (common) inline headers.
    """

    def __init__(self, conf, clean_subject=False, use_unix_time=False):
        """Set parsing rules."""
        super().__init__(conf)
        self.clean_subject = clean_subject
        self.use_unix_time = use_unix_time
        self.start_date = datetime.datetime.fromtimestamp(conf.get('data', 'time_min'))
        self.end_date = datetime.datetime.fromtimestamp(conf.get('data', 'time_max'))

    def prepare_header_string(self, text):
        """Remove whitespace, newlines and other noise."""
        text = re.sub(r'.*----- ?original message ?-----', '', text, flags=re.IGNORECASE)
        text = re.sub(r'.*-{5,} forwarded by.+-{5,}', '', text, 0, re.IGNORECASE | re.DOTALL)  # remove 2ndary header
        text = re.sub(r'^(\s|>)+', '', text, flags=re.MULTILINE)  # remove leading > and whitespace
        text = re.sub(r'\s+', ' ', text)  # normalize whitespace
        text = text.replace('*', '')
        text = text.strip(whitespace)

        return text

    def transform_header_string(self, header_string):
        """Split a string that is likely a header into its fields."""
        header_string = self.prepare_header_string(header_string)

        separator_re = re.compile(r'\s((?=(x-)?from:\s)|(?=((x|reply)-)?to:\s)|(?=(x-)?b?cc:\s)|(?=date:\s)|(?=sent(-by)?:\s)|(?=subject:\s))', re.IGNORECASE)  # NOQA
        header_fields = separator_re.split(header_string)  # split into separate headers
        header_fields = [header_field for header_field in header_fields if header_field]  # filter none and empty
        for index, header_field in enumerate(header_fields):
            if header_field[-1:] == ':':
                header_field += ' '  # if key without value is included in header, add empty value
            header_fields[index] = header_field.split(': ', 1)  # make key value pair of a header

        return header_fields

    def get_header_value(self, transformed_header, field):
        """Get value from a transformed header list."""
        field = [header_value for header_value in transformed_header if header_value[0].lower() == field.lower()]
        try:
            return field[0][1]
        except IndexError:
            return ''

    def clean_email(self, email_string):
        """Clean email address."""
        email = email_string.replace('[mailto:', '').replace(']', '').replace('"', '').replace("'", '')
        if email.count('@') > 1 and re.search(r'<[^>]+>', email):
            email = re.sub(r'<[^>]+>', '', email)  # case: some_email@a.com<mailto:some_email@a.com>
        email = email.replace('<', '').replace('>', '')
        if re.search(r'\S+@\S+\.\S{2,}', email):
            email = re.search(r'\S+@\S+\.\S{2,}', email).group(0)
        else:
            email = ''

        return unquote(email.lower())

    def clean_name(self, name_string):
        """Normalize and clean a name. Lastname, Firstname becomes to Fn Ln."""
        name = re.sub(r'(<.+>)|(\[.+\])', '', name_string)  # remove [FI] flags and similar
        name = re.sub(r'\S+@\S+\.\S{2,}', '', name)  # remove email
        name = re.sub(r'(?<=\w)(/|@).*', '', name)  # normalize weird enron names (beau ratliff/hou/ees@ees)
        name = name.replace('"', '').replace("'", '').split(',')
        name.reverse()
        name = ' '.join(name).strip(whitespace)
        name = re.sub(r'\s+', ' ', name)

        if not name:  # parse name from email address if else unavailable
            email_without_domain = re.sub(r'@.+', '', self.clean_email(name_string))
            if '.' in email_without_domain:
                name_parts = [part for part in email_without_domain.split('.') if part]
                name = ' '.join(name_parts)

        return name.title()

    def parse_correspondent(self, correspondent_string):
        """Given a string containig name and/or email, return a correspondent dict."""
        return {'name': self.clean_name(correspondent_string),
                'email': self.clean_email(correspondent_string)}

    def parse_recipients(self, field_string, kind='to', delimiter=','):
        """Parse a list (in string format) to correspondents."""
        if delimiter != ';' and ';' not in field_string:  # make ; seperator for every correspondent list
            for index, char in enumerate(field_string):
                if char == delimiter and field_string[:index].count('"') % 2 == 0:
                    new_field_string = list(field_string)
                    new_field_string[index] = ';'
                    field_string = ''.join(new_field_string)

        field_splitted = field_string.split(';')

        recipients = []
        for recipient in field_splitted:
            recipient_obj = self.parse_correspondent(recipient)
            recipient_obj['type'] = kind
            recipients.append(recipient_obj.copy())

        return recipients

    def parse_date(self, date_string):
        """Normalize date from a string. If self.use_unix_time set return timestamp."""
        date = re.sub(r'\(\w+\)', '', date_string).strip(whitespace)  # remove additional tz in brackets
        try:
            date = dateparser.parse(date)
        except Exception:
            return '', False

        if date is None:
            return '', False

        date, changed = self.normalize_date(date)

        return date.strftime('%Y-%m-%dT%H:%M:%S') + 'Z' if not self.use_unix_time else date.timestamp(), changed

    def normalize_date(self, original_date):
        """Normalize date to given period of time."""
        date = original_date.replace(tzinfo=None)

        if self.start_date:
            if date < self.start_date:
                return self.start_date, True

        if self.end_date:
            if date > self.end_date:
                return self.end_date, True

        return original_date, False

    def parse_subject(self, subject_string):
        """Clean subject line from RE:, AW: etc if self.clean_subject=True."""
        if self.clean_subject:
            subject_string = re.sub(r'((fwd?: )|(re: ))+', subject_string, flags=re.IGNORECASE)

        return subject_string.strip(whitespace)

    def parse_header(self, header_string):
        """Given a message object, parse all relevant metadata and return them in a header dict."""
        header = {'sender': {'name': '', 'email': ''},
                  'recipients': [],
                  'date': None,
                  'date_changed': '',
                  'subject': ''}

        headers = self.transform_header_string(header_string)

        if self.get_header_value(headers, 'from'):
            header['sender'] = self.parse_correspondent(self.get_header_value(headers, 'from'))
        elif headers and len(headers[0]) == 1:  # special header, missing from key in first line
            sender = re.sub(r'(on )?\d{2}/\d{2}/\d{2,4}\s\d{2}:\d{2}(:\d{2})?\s?(am|pm)?', '',
                            headers[0][0], flags=re.IGNORECASE)  # rm date
            header['sender'] = self.parse_correspondent(sender)

        delimiter = ',' if 'Original Message' not in header_string else ';'  # catch case 'to: lastname, firstname'
        if self.get_header_value(headers, 'to'):
            header['recipients'] += self.parse_recipients(self.get_header_value(headers, 'to'),
                                                          kind='to', delimiter=delimiter)
        if self.get_header_value(headers, 'cc'):
            header['recipients'] += self.parse_recipients(self.get_header_value(headers, 'cc'),
                                                          kind='cc', delimiter=delimiter)
        if self.get_header_value(headers, 'bcc'):
            header['recipients'] += self.parse_recipients(self.get_header_value(headers, 'to'),
                                                          kind='bcc', delimiter=delimiter)

        if self.get_header_value(headers, 'date'):
            header['date'], header['date_changed'] = self.parse_date(self.get_header_value(headers, 'date'))
        elif self.get_header_value(headers, 'sent'):
            header['date'], header['date_changed'] = self.parse_date(self.get_header_value(headers, 'sent'))
        elif headers:
            date = re.search(r'\d{2}/\d{2}/\d{2,4}\s\d{2}:\d{2}(:\d{2})?\s?(am|pm)?',
                             self.prepare_header_string(header_string), flags=re.IGNORECASE)  # get date
            if date is not None:
                header['date'], header['date_changed'] = self.parse_date(date.group(0))

        if self.get_header_value(headers, 'subject'):
            header['subject'] = self.parse_subject(self.get_header_value(headers, 'subject'))

        return header

    def run_on_document(self, raw_message):
        """Get body and header information for a leuchtturm document."""
        document = json.loads(raw_message)

        if 'parts' in document:
            for part in document['parts']:
                part['header'] = self.parse_header(part['header'])
        else:
            document['header'] = self.parse_header(document['header'])

        return json.dumps(document, ensure_ascii=False)

    def run(self, rdd):
        """Run task in spark context."""
        return rdd.map(lambda x: self.run_on_document(x))


class LanguageDetection(Pipe):
    """Detect language of text.

    Given a json field name, this task can detect its language.
    Add 2 char language code indicating language in field lang.
    """

    def __init__(self, conf, read_from='text_clean'):
        """Set lang detect params."""
        super().__init__(conf)
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

        return json.dumps(document, ensure_ascii=False)

    def run(self, rdd):
        """Run task in spark context."""
        return rdd.map(lambda x: self.run_on_document(x))


class TextCleaning(Pipe):
    """Clean text from everythin that could cause problems to text mining algorithms.

    Convert to ascii, remove punctuation and overly used whitespace.
    Clean from inline headers and other email specific 'noise'.
    """

    def __init__(
            self,
            conf,
            read_from='body',
            write_to='text_clean',
            write_to_original_ws='text_clean_original_ws',
            readable=True
    ):
        """Set params."""
        super().__init__(conf)
        self.read_from = read_from
        self.write_to = write_to
        self.write_to_original_ws = write_to_original_ws
        self.readable = readable

    def convert_to_ascii(self, text):
        """Replace unicode chars with their closest ascii char."""
        text = textacy.preprocess.fix_bad_unicode(text, normalization='NFC')
        text = textacy.preprocess.transliterate_unicode(text)
        text = textacy.preprocess.unpack_contractions(text)
        return text

    def remove_header(self, text):
        """Remove email and enron specific noise from texts."""
        headers = [r'^(((subject:)|(from:)|(sent:)|(date:)|(to:)|(cc:))(\s.*\n)){3,}\s+',
                   r'----- forwarded.*((from:.*)|subject:(.)*|to:(.)*|sent:(.)*|cc:(.)*|\n)*\n',
                   r'-----\s?original message\s?-----',
                   r'(\*|=|-){40,}\s(.|\n)+(\*|=|-){40,}\s']

        text_clean = text
        for header in headers:
            text_clean = re.sub(header, '', text_clean, 0, re.MULTILINE | re.IGNORECASE | re.UNICODE)

        edrm_footer = ('***********\r\nEDRM Enron Email Data Set has been produced in EML, PST and NSF format by ZL '
                       'Technologies, Inc. This Data Set is licensed under a Creative Commons Attribution 3.0 United '
                       'States License <http://creativecommons.org/licenses/by/3.0/us/> . To provide attribution, '
                       'please cite to \"ZL Technologies, Inc. (http://www.zlti.com).\"\r\n***********')

        text_clean = text_clean.replace(edrm_footer, '')

        return text_clean

    def remove_strict(self, text):
        """Remove everything(!) that could disturb tm tasks. Won't be readable afterwards."""
        return textacy.preprocess.preprocess_text(text, no_urls=True, no_emails=True, no_phone_numbers=True,
                                                  no_numbers=True, no_currency_symbols=True, no_punct=True)

    def run_on_document(self, raw_message):
        """Transform a document into clean text."""
        document = json.loads(raw_message)
        clean = document[self.read_from]

        for func in [self.remove_header, self.convert_to_ascii]:
            clean = func(clean)

        clean_original_ws = clean
        clean = textacy.preprocess.normalize_whitespace(clean)  # Replace 2+ spaces/newlines with 1 char

        if not self.readable:
            clean_original_ws = self.remove_strict(clean_original_ws)
            clean = self.remove_strict(clean)

        document[self.write_to_original_ws] = clean_original_ws
        document[self.write_to] = clean
        return json.dumps(document, ensure_ascii=False)

    def run(self, rdd):
        """Run pipe in spark context."""
        return rdd.map(lambda x: self.run_on_document(x))
