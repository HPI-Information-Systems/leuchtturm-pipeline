"""Preprocess emails."""

from email import message_from_string, utils
import re


def parse_raw_emails(df):
    df['date'] = parse_date(df['email'])
    df['subject'] = parse_subject(df['email'])
    df['from_name'] = parse_from_name(df['email'])
    df['from_email'] = parse_from_email(df['email'])
    df['to_names'] = parse_to_names(df['email'])
    df['to_emails'] = parse_to_emails(df['email'])
    df['cc_names'] = parse_cc_names(df['email'])
    df['cc_emails'] = parse_cc_emails(df['email'])
    df['bcc_names'] = parse_bcc_names(df['email'])
    df['bcc_emails'] = parse_bcc_emails(df['email'])
    df['reply_to_name'] = parse_reply_to_name(df['email'])
    df['reply_to_email'] = parse_reply_to_email(df['email'])
    df['body'] = parse_body(df['email'])

    return df.reset_index(drop=True)


def parse_subject(df):
    col = []
    for email in df:
        col.append(message_from_string(email).get('subject', ''))

    return col


def parse_date(df):
    col = []
    for email in df:
        col.append(message_from_string(email).get('date', ''))

    return col


def parse_from_email(df):
    col = []
    for email in df:
        col.append(utils.parseaddr(message_from_string(email).get('from', ''))[1])

    return col


def parse_from_name(df):
    col = []
    for email in df:
        col.append(utils.parseaddr(message_from_string(email).get('from', ''))[0])

    return col


def parse_reply_to_email(df):
    col = []
    for email in df:
        col.append(utils.parseaddr(message_from_string(email).get('reply-to', ''))[1])

    return col


def parse_reply_to_name(df):
    col = []
    for email in df:
        col.append(utils.parseaddr(message_from_string(email).get('reply-to', ''))[0])

    return col


def parse_to_emails(df):
    col = []
    for email in df:
        col.append(','.join([elem[1] for elem in utils.getaddresses(message_from_string(email).get_all('to', []))]))

    return col


def parse_to_names(df):
    col = []
    for email in df:
        col.append(','.join([elem[0] for elem in utils.getaddresses(message_from_string(email).get_all('to', []))]))

    return col


def parse_cc_emails(df):
    col = []
    for email in df:
        col.append(','.join([elem[1] for elem in utils.getaddresses(message_from_string(email).get_all('cc', []))]))

    return col


def parse_cc_names(df):
    col = []
    for email in df:
        col.append(','.join([elem[0] for elem in utils.getaddresses(message_from_string(email).get_all('cc', []))]))

    return col


def parse_bcc_emails(df):
    col = []
    for email in df:
        col.append(','.join([elem[1] for elem in utils.getaddresses(message_from_string(email).get_all('bcc', []))]))

    return col


def parse_bcc_names(df):
    col = []
    for email in df:
        col.append(','.join([elem[0] for elem in utils.getaddresses(message_from_string(email).get_all('bcc', []))]))

    return col


def parse_body(df):
    def decode_part(text, encoding=None):
        if encoding is None:
            encoding = 'utf-8'

        return text.decode(encoding, 'ignore')

    def remove_inline_headers(text):
        forwarded_by_heuristic = r'(.*-{3}.*Forwarded by((\n|.)*?)Subject:.*)'
        begin_forwarded_message_heuristic = r'(.*Begin forwarded message:((\n|.)*?)To:.*)'
        original_message_heuristic = r'(.*-{3}.*Original Message((\n|.)*?)Subject:.*)'
        reply_seperator_heuristic = r'(.*_{3}.*Reply Separator((\n|.)*?)Date.*)'
        date_to_subject_heuristic = r'(.*\n.*(on )?\d{2}\/\d{2}\/\d{2,4}\s\d{2}:\d{2}(:\d{2})?\s?(AM|PM|am|pm)?.*\n.*(\n.*)?To: (\n|.)*?Subject: .*)' # NOQA
        from_to_subject_heuristic = r'(.*From:((\n|.)*?)Subject:.*)'

        header_regex = re.compile('(%s|%s|%s|%s|%s|%s)' % (
            forwarded_by_heuristic,
            begin_forwarded_message_heuristic,
            original_message_heuristic,
            reply_seperator_heuristic,
            date_to_subject_heuristic,
            from_to_subject_heuristic
        ))

        return header_regex.sub('', text)

    def get_body(raw_email, remove_inline_header=True):
        body_text = ''
        for part in message_from_string(raw_email).walk():
            charset = part.get_content_charset()
            if part.get_content_type() == 'text/plain' or part.get_content_type() == 'text/html':
                text = part.get_payload(decode=True)
                body_text = decode_part(text, encoding=charset)
                break

        if remove_inline_header:
            body_text = remove_inline_headers(body_text)

        return body_text

    col = []
    for email in df:
        col.append(get_body(email))

    return col
