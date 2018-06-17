"""Methods for email preprocessing, feature engineering, selection, transformation and classification."""

from datetime import datetime
from email import message_from_string, utils
import os
import re
from string import whitespace

import dateparser
import pickle
from html2text import HTML2Text
from nltk import tokenize
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from textacy.preprocess import preprocess_text


class Loading:

    @staticmethod
    def read_from_directory_structure(root_path):
        columns = ['email', 'label']
        df = pd.DataFrame(columns=columns)

        for root, _, files in os.walk(root_path):
            for file in [file for file in files if not file.startswith(('.', '__'))]:
                with open(root + os.sep + file, encoding='utf-8', errors='replace') as f:
                    email = f.read()
                    label = re.sub(r'.+/', '', root)

                    new_row = pd.DataFrame(
                        data=[[email, label]],
                        columns=columns
                    )

                    df = df.append(new_row).reset_index(drop=True)

        return df

    @staticmethod
    def read_from_anno_files(root_path, suffix_raw='.txt', suffix_anno='.cats'):
        columns = ['email', 'coarse_genre', 'included_information', 'primary_topics', 'emotional_tone']
        df = pd.DataFrame(columns=columns)

        for root, _, files in os.walk(root_path):
            files = [file for file in files if not file.startswith(('.', '__'))]
            files = list(set([re.sub(r'\..{3,}', '', file) for file in files]))

            for file in files:
                with open(root + os.sep + file + suffix_raw, encoding='utf-8', errors='ignore') as f:
                    email = f.read()

                with open(root + os.sep + file + suffix_anno, encoding='utf-8', errors='ignore') as f:
                    labels = dict([label.split(',', maxsplit=1) for label in f.read().splitlines()])

                    coarse_genre = re.sub(r',.*', '', labels['1'])
                    included_information = re.sub(r',.*', '', labels['2']) if '2' in labels else '0'
                    primary_topics = re.sub(r',.*', '', labels['3']) if '3' in labels else '0'
                    emotional_tone = re.sub(r',.*', '', labels['4']) if '4' in labels else '0'

                new_row = pd.DataFrame(
                    data=[[email, coarse_genre, included_information, primary_topics, emotional_tone]],
                    columns=columns
                )

                df = df.append(new_row).reset_index(drop=True)

        return df


class Preprocessing:

    @staticmethod
    def parse_raw_emails(df):
        df['date'] = Preprocessing.parse_date(df['email'])
        df['subject'] = Preprocessing.parse_subject(df['email'])
        df['from_name'] = Preprocessing.parse_from_name(df['email'])
        df['from_email'] = Preprocessing.parse_from_email(df['email'])
        df['to_names'] = Preprocessing.parse_to_names(df['email'])
        df['to_emails'] = Preprocessing.parse_to_emails(df['email'])
        df['cc_names'] = Preprocessing.parse_cc_names(df['email'])
        df['cc_emails'] = Preprocessing.parse_cc_emails(df['email'])
        df['bcc_names'] = Preprocessing.parse_bcc_names(df['email'])
        df['bcc_emails'] = Preprocessing.parse_bcc_emails(df['email'])
        df['reply_to_name'] = Preprocessing.parse_reply_to_name(df['email'])
        df['reply_to_email'] = Preprocessing.parse_reply_to_email(df['email'])
        df['body'] = Preprocessing.parse_body(df['email'])

        return df.reset_index(drop=True)

    @staticmethod
    def parse_subject(df):
        col = []
        for email in df:
            col.append(message_from_string(email).get('subject', ''))

        return col

    @staticmethod        
    def parse_date(df):
        # TODO parse date
        col = []
        for email in df:
            date = re.sub(r'\(\w+\)', '', message_from_string(email).get('date', '')).strip(whitespace)
            try:
                date = dateparser.parse(date)
                col.append(date.timestamp())
            except Exception:
                col.append(0)

        return col

    @staticmethod
    def parse_from_email(df):
        col = []
        for email in df:
            col.append(utils.parseaddr(message_from_string(email).get('from', ''))[1])

        return col

    @staticmethod
    def parse_from_name(df):
        # TODO parse from email if nil
        col = []
        for email in df:
            col.append(utils.parseaddr(message_from_string(email).get('from', ''))[0])

        return col

    @staticmethod
    def parse_reply_to_email(df):
        col = []
        for email in df:
            col.append(utils.parseaddr(message_from_string(email).get('reply-to', ''))[1])

        return col

    @staticmethod
    def parse_reply_to_name(df):
        col = []
        for email in df:
            col.append(utils.parseaddr(message_from_string(email).get('reply-to', ''))[0])

        return col

    @staticmethod
    def parse_to_emails(df):
        col = []
        for email in df:
            col.append(';'.join([elem[1] for elem in utils.getaddresses(message_from_string(email).get_all('to', []))]))

        return col

    @staticmethod
    def parse_to_names(df):
        col = []
        for email in df:
            col.append(';'.join([elem[0] for elem in utils.getaddresses(message_from_string(email).get_all('to', []))]))

        return col

    @staticmethod
    def parse_cc_emails(df):
        col = []
        for email in df:
            col.append(';'.join([elem[1] for elem in utils.getaddresses(message_from_string(email).get_all('cc', []))]))

        return col

    @staticmethod
    def parse_cc_names(df):
        col = []
        for email in df:
            col.append(';'.join([elem[0] for elem in utils.getaddresses(message_from_string(email).get_all('cc', []))]))

        return col

    @staticmethod
    def parse_bcc_emails(df):
        col = []
        for email in df:
            col.append(';'.join([elem[1] for elem in utils.getaddresses(message_from_string(email).get_all('bcc', []))]))

        return col

    @staticmethod
    def parse_bcc_names(df):
        col = []
        for email in df:
            col.append(';'.join([elem[0] for elem in utils.getaddresses(message_from_string(email).get_all('bcc', []))]))

        return col

    @staticmethod
    def parse_body(df):
        def remove_html_tags(text):
            h = HTML2Text()
            h.ignore_links = True
            h.ignore_emphasis = True
            h.ignore_images = True

            return h.handle(text)

        def decode_part(text, encoding=None):
            if encoding is None:
                encoding = 'utf-8'

            try:
                text = text.decode(encoding, 'replace')
            except LookupError:
                text = text.decode('utf-8', 'replace')

            if re.search(r'<[^>]+>', text) and re.search(r'</[^>]+>', text):
                text = remove_html_tags(text)

            return text

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

            # remove inline class labels
            body_text = body_text.replace('CLOSE_PERSONAL', '').replace('CORE_BUSINESS', '')

            return body_text.strip(whitespace)

        col = []
        for email in df:
            col.append(get_body(email))

        return col


class Features:

    _integer_columns = ['num_to', 'num_cc', 'num_bcc', 'num_sentences', 'num_words_body', 'num_words_subj',
                        'num_long_words_body', 'num_long_words_subj', 'num_chars_body', 'num_long_words_subj',
                        'num_chars_body', 'num_chars_subj', 'num_unique_words', 'recipient_first_name_in_salutation',
                        'recipient_last_name_in_salutation', 'sender_first_name_in_email', 'sender_last_name_in_email',
                        'recipient_first_name_in_salutation', 'recipient_last_name_in_salutation', 'sender_first_name_in_email',
                        'sender_last_name_in_email', 'sent_at_weekend', 'sent_during_business_hours', 'has_reply_to',
                        'is_reply', 'is_fwd', 'has_html_tag', 'has_css_selector', 'num_links', 'num_phone_numbers',
                        'num_email_adresses', 'num_numbers', 'has_unsubscribe_option', 'sender_is_noreply', 'weekday']
    _float_columns = ['med_sentence_len', 'automated_readabilty_index', 'lix_score']

    _one_hot_encode = []
    _label_encode = []

    _drop = ['email', 'date', 'subject', 'subject_clean', 'subject_stemmed', 'body', 'body_clean', 'body_stemmed',
             'from_name', 'from_email', 'to_names', 'to_emails', 'cc_names', 'cc_emails', 'bcc_names', 'bcc_emails',
             'reply_to_name', 'reply_to_email']

    @staticmethod
    def add_all_features(df):
        df['body_clean'] = Features.clean_text(df, 'body')
        df['body_stemmed'] = Features.stem_text(df, 'body_clean')
        df['subject_clean'] = Features.clean_text(df, 'subject')
        df['subject_stemmed'] = Features.stem_text(df, 'subject_clean')
        df['num_to'] = Features.add_num_of(df, 'to_emails')
        df['num_cc'] = Features.add_num_of(df, 'cc_emails')
        df['num_bcc'] = Features.add_num_of(df, 'bcc_emails')
        df['num_sentences'] = Features.add_num_sentences(df, 'body_clean')
        df['num_words_body'] = Features.add_num_words(df, 'body_clean')
        df['num_words_subj'] = Features.add_num_words(df, 'subject')
        df['num_long_words_body'] = Features.add_num_long_words(df, 'body_clean')
        df['num_long_words_subj'] = Features.add_num_long_words(df, 'subject_clean')
        df['num_chars_body'] = Features.add_num_chars(df, 'body_clean')
        df['num_chars_subj'] = Features.add_num_chars(df, 'subject_clean')
        df['num_unique_words'] = Features.add_num_unique_words(df, 'body_clean')
        df['med_sentence_len'] = Features.add_med_sentence_len(df, 'body_clean')
        df['automated_readabilty_index'] = Features.add_automated_readabilty_index(df, 'body_clean')
        df['lix_score'] = Features.add_lix_score(df, 'body_clean')
        df['recipient_first_name_in_salutation'] = Features.add_recipient_first_name_in_salutation(df)
        df['recipient_last_name_in_salutation'] = Features.add_recipient_last_name_in_salutation(df)
        df['sender_first_name_in_email'] = Features.add_sender_first_name_in_email(df)
        df['sender_last_name_in_email'] = Features.add_sender_last_name_in_email(df)
        df['weekday'] = Features.add_weekday(df)
        df['sent_at_weekend'] = Features.add_sent_at_weekend(df)
        df['sent_during_business_hours'] = Features.add_sent_during_business_hours(df)
        # TODO domain of sender and recipients same
        df['has_reply_to'] = Features.add_regex_matches_field(df, r'.+', 'reply_to_email')
        df['is_reply'] = Features.add_regex_matches_field(df, r're: ', 'subject')
        df['is_fwd'] = Features.add_regex_matches_field(df, r'fwd?: ', 'subject')
        df['has_html_tag'] = Features.add_regex_matches_field(df, r'<([^\s]+)(\s[^>]*?)?(?<!/)>', 'email')
        df['has_css_selector'] = Features.add_regex_matches_field(df, r'(\#|\.| ).+\{[^}]+:[^}]+}', 'email')
        df['num_links'] = Features.add_count_substring_occurences(df, '*url*', 'body_clean')
        df['num_phone_numbers'] = Features.add_count_substring_occurences(df, '*phone*', 'body_clean')
        df['num_email_adresses'] = Features.add_count_substring_occurences(df, '*email*', 'body_clean')
        df['num_numbers'] = Features.add_count_substring_occurences(df, '*num*', 'body_clean')
        df['has_unsubscribe_option'] = Features.add_regex_matches_field(df,
                                                                        r'(unsubscribe)|(e-?mail.preferences)',
                                                                        'body_clean')
        # TODO attachments? (num, type)
        # TODO greeting formal?
        df['sender_is_noreply'] = Features.add_regex_matches_field(df,
                                                                   r'(not?-?reply)|(newsletter)|(info)|(notif(i|y)?)',
                                                                   'from_email')
        # TODO mobile signature

        return df

    @staticmethod
    def transform_features(df):
        global _integer_columns
        global _float_columns
        global _one_hot_encode
        global _label_encode

        # ensure correct data types
        for column in Features._integer_columns:
            df[column] = df[column].astype(int)

        for column in Features._float_columns:
            df[column] = df[column].astype(float)

        # one-hot-encode
        df = pd.get_dummies(data=df, prefix=Features._one_hot_encode, columns=Features._one_hot_encode)

        # label-encode
        for column in Features._label_encode:
            encoder = LabelEncoder().fit(df[column])
            df[column] = encoder.transform(df[column])

        return df

    @staticmethod
    def drop_features(df, drop=[]):
        # append columns that are always supposed to be dropped (defined in _drop, see above)
        drop = drop + Features._drop

        # check if columns are stil in the dataFrame
        data_columns = list(df)
        drop_hot_encoded = []
        for to_be_dropped in drop:
            drop_hot_encoded = drop_hot_encoded + [column for column in data_columns if to_be_dropped in column]

        # drop columns that may not be needed any more
        df.drop(drop_hot_encoded, inplace=True, axis=1)

        return df

    @staticmethod
    def clean_text(df, field):
        col = []
        for text in df[field]:
            text = preprocess_text(
                text,
                fix_unicode=True,
                lowercase=True,
                transliterate=True,
                no_urls=True,
                no_emails=True,
                no_phone_numbers=True,
                no_numbers=True,
                no_currency_symbols=True,
                no_contractions=True,
                no_accents=True,
                no_punct=False
            )
            col.append(text)

        return col

    @staticmethod
    def stem_text(df, field):
        stemmer = SnowballStemmer('english')
        sw = set(stopwords.words('english'))
        col = []
        for text in df[field]:
            tokens = tokenize.word_tokenize(text)
            stemmed_tokens = [stemmer.stem(token) for token in tokens if token not in sw]
            col.append(' '.join(stemmed_tokens))

        return col

    @staticmethod
    def add_num_of(df, field):
        return [len(field.split(';')) for field in df[field]]

    @staticmethod
    def add_vectorized_field(df, field, new_field, vectorizer):
        matrix = vectorizer.transform(df[field]).toarray()
        labels = [new_field + '_' + str(i) for i in range(0, len(matrix[0]))]
        df_matrix = pd.DataFrame(data=matrix, columns=labels)

        return pd.concat([df, df_matrix], axis=1)

    @staticmethod
    def add_num_sentences(df, field):
        return [len(tokenize.sent_tokenize(text)) for text in df[field]]

    @staticmethod
    def add_num_words(df, field):
        return [len(tokenize.word_tokenize(text)) for text in df[field]]

    @staticmethod
    def add_num_chars(df, field):
        return [len(text) for text in df[field]]

    @staticmethod
    def add_num_unique_words(df, field):
        col = []
        sw = set(stopwords.words('english'))
        for text in df[field]:
            num = len(list(set([word for word in tokenize.word_tokenize(text) if word not in sw])))
            col.append(num)

        return col

    @staticmethod
    def add_med_sentence_len(df, field):
        col = []
        for text in df[field]:
            num = [len(w) for w in tokenize.sent_tokenize(text)]
            median = np.median(num)
            if median > 0:
                col.append(median)
            else:
                col.append(0)

        return col

    @staticmethod
    def add_num_long_words(df, field):
        col = []
        for text in df[field]:
            num = len([w for w in tokenize.word_tokenize(text) if len(w) >= 6])
            col.append(num)

        return col

    @staticmethod
    def add_automated_readabilty_index(df, field):
        # https://en.wikipedia.org/wiki/Automated_readability_index
        col = []
        for text in df[field]:
            n_chars = len(text)
            n_words = len(tokenize.word_tokenize(text))
            n_sents = len(tokenize.sent_tokenize(text))
            try:
                col.append((4.71 * n_chars / n_words) + (0.5 * n_words / n_sents) - 21.43)
            except ZeroDivisionError:
                col.append(0)

        return col

    @staticmethod
    def add_lix_score(df, field):
        # https://en.wikipedia.org/wiki/Lix_(readability_test)
        col = []
        for text in df[field]:
            n_words = len(tokenize.word_tokenize(text))
            n_long_words = len([word for word in tokenize.word_tokenize(text) if len(word) > 6])
            n_sents = len(tokenize.sent_tokenize(text))
            try:
                col.append((n_words / n_sents) + (100 * n_long_words / n_words))
            except ZeroDivisionError:
                col.append(0)

        return col

    @staticmethod
    def add_recipient_first_name_in_salutation(df):
        col = []
        for idx, text in enumerate(df['body']):
            try:
                salutation = text.splitlines()[0]
                recipient_names = df['to_names'][idx].split(';')
                recipient_first_names = [name.split()[0] for name in recipient_names]
                col.append(np.mean([1 if name in salutation else 0 for name in recipient_first_names]))
            except IndexError:
                col.append(0)

        return col

    @staticmethod
    def add_recipient_last_name_in_salutation(df):
        col = []
        for idx, text in enumerate(df['body']):
            try:
                salutation = text.splitlines()[0]
                recipient_names = df['to_names'][idx].split(';')
                recipient_last_names = [name.split()[1] for name in recipient_names]
                col.append(np.mean([1 if name in salutation else 0 for name in recipient_last_names]))
            except IndexError:
                col.append(0)

        return col

    @staticmethod
    def add_sender_first_name_in_email(df):
        col = []
        for idx, text in enumerate(df['body']):
            try:
                first_name = df['from_name'][idx].split()[0]
                col.append(1 if first_name in text else 0)
            except IndexError:
                col.append(0)

        return col

    @staticmethod
    def add_sender_last_name_in_email(df):
        col = []
        for idx, text in enumerate(df['body']):
            try:
                last_name = df['from_name'][idx].split()[1]
                col.append(1 if last_name in text else 0)
            except IndexError:
                col.append(0)

        return col

    @staticmethod
    def add_regex_matches_field(df, regex, field):
        return [1 if re.search(regex, text, re.I) else 0 for text in df[field]]

    @staticmethod
    def add_count_substring_occurences(df, substring, field):
        return [text.lower().count(substring.lower()) for text in df[field]]

    @staticmethod
    def add_weekday(df):
        col = []
        for date in df['date']:
            try:
                # 0 is sunday and 6 is saturday
                weekday = datetime.fromtimestamp(date).strftime('%w')
                col.append(weekday)
            except Exception:
                col.append(7)

        return col

    @staticmethod
    def add_sent_at_weekend(df):
        col = []
        for date in df['date']:
            try:
                # 0 is sunday and 6 is saturday
                weekday = datetime.fromtimestamp(date).strftime('%w')
                col.append(1 if weekday == 0 or weekday == 6 else 0)
            except Exception:
                col.append(0)

        return col

    @staticmethod
    def add_sent_during_business_hours(df):
        col = []
        for date in df['date']:
            try:
                # assuming enron staff had 9 to 5 jobs and no work on sundays ;-)
                hour = datetime.fromtimestamp(date).strftime('%-H')
                weekday = datetime.fromtimestamp(date).strftime('%w')
                col.append(1 if hour >= 9 and hour <= 16 and weekday != 0 else 0)
            except Exception:
                col.append(0)

        return col


class EmailClassificationTool:

    def __init__(self, clf_3, clf_genre, vectorizer_body, vectorizer_subject, labels_3, labels_genre):
        super().__init__()
        self.classifier_3 = clf_3
        self.classifier_genre = clf_genre
        self.vectorizer_body = vectorizer_body
        self.vectorizer_subject = vectorizer_subject
        self.labels_3 = labels_3
        self.labels_genre = labels_genre

    @staticmethod
    def load_from_file(path):
        with open(path, 'rb') as f:
            return pickle.load(f)

    def dump_as_file(self, path):
        if not self.classifier_3 or not self.vectorizer_body or not self.vectorizer_subject or not self.labels_3:
            raise Exception('EmailClassificationTool has not been created properly.')

        with open(path, 'wb') as f:
            pickle.dump(self, f)

    def create_df(self, text):
        return pd.DataFrame(data=[[text]], columns=['email'])

    def preprocess(self, df):
        return Preprocessing.parse_raw_emails(df)

    def add_features(self, df):
        return Features.add_all_features(df)

    def transform(self, df):
        return Features.transform_features(df)

    def vectorize_field(self, df, field, new_field, vectorizer):
        return Features.add_vectorized_field(df, field, new_field, vectorizer)

    def drop_features(self, df):
        return Features.drop_features(df)

    def prepare_email(self, text):
        df = self.create_df(text)
        df = self.preprocess(df)
        df = self.add_features(df)
        df = self.transform(df)
        df = self.vectorize_field(df, 'body', 'b', self.vectorizer_body)
        df = self.vectorize_field(df, 'subject', 's', self.vectorizer_subject)
        df = self.drop_features(df)

        return df

    def translate_label(self, number, encoder):
        return encoder.inverse_transform(number)

    def predict_top_category(self, text):
        df = self.prepare_email(text)
        label = self.classifier_3.predict(df)[0]

        return self.translate_label(label, self.labels_3)

    def predict_proba(self, text):
        df = self.prepare_email(text)
        probabilities = self.classifier_3.predict_proba(df)[0]
        labels = self.labels_3.classes_

        return dict(zip(labels, probabilities))

    def predict(self, text):
        df = self.prepare_email(text)

        label_3 = self.classifier_3.predict(df)[0]
        probabilities_3 = dict(zip(self.labels_3.classes_, self.classifier_3.predict_proba(df)[0]))

        obj = {
            'top_category': self.translate_label(label_3, self.labels_3),
            'prob_category': probabilities_3
        }

        if obj['top_category'] == 'business':
            label_genre = self.classifier_genre.predict(df)[0]
            probabilities_genre = zip(self.labels_genre.classes_, self.classifier_genre.predict_proba(df)[0])
            obj['top_subcategory'] = self.translate_label(label_genre, self.labels_genre)
            obj['prob_subcategory'] = probabilities_genre

        return obj
