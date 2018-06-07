from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import PCA

import pandas as pd
import numpy as np
from nltk.stem.porter import *

import re
from html2text import HTML2Text
from textacy.preprocess import preprocess_text
from email import message_from_string, utils
from string import whitespace
import dateparser
from datetime import datetime

class Preprocessing:
    # Preprocessing is used to extract all information and fields that are / might be needed to extract the features
    @staticmethod
    def preprocess_emails(df):
        d = {
            'body_clean': [],
            'body_length': [],

            'subject_clean': [],
            'subject_length': [],

            'sender': [],
            'sender_domain': [],
            'sender_is_internal': [],

            'recipients': [],
            'recipients_cc': [],
            'recipients_domains': [],
            'recipients_domains_most_frequent': [],
            'recipients_amount': [],
            'recipients_is_single': [],
            'recipients_is_internal': [],

            'email_is_internal': [],
            'email_is_thread': [],

            'date': [],
            'date_month': [],
            'date_year': [],
            'date_day': [],
            'date_hour': [],
            'date_weekday': [],

            'sent_at_weekend': [],
            'sent_during_business_hours': []
        }

        for row in df.iterrows():
            message = message_from_string(row[1].raw)

            body_complete = Preprocessing.remove_html_tags(Preprocessing.parse_body(message), check_for_tags=True)
            body_without_inline_headers = Preprocessing.remove_inline_headers(body_complete)
            body_clean = Preprocessing.clean_text(Preprocessing.remove_html_tags(body_without_inline_headers, check_for_tags=False))
            d['body_clean'].append(body_clean if body_clean else ' ')
            d['body_length'].append(len(body_complete))
            d['email_is_thread'].append(len(body_complete) > len(body_without_inline_headers))

            subject_complete = message.get('subject', '')
            subject_clean = Preprocessing.clean_text(subject_complete) if subject_complete else ' '
            d['subject_clean'].append(subject_clean)
            d['subject_length'].append(len(subject_complete))

            sender = Preprocessing.parse_from_email(message)
            d['sender'].append(sender if sender else 'UNKNOWN')
            sender_domain = Preprocessing.get_email_domain(sender)
            d['sender_domain'].append(sender_domain)
            sender_is_internal = 'enron' in sender_domain
            d['sender_is_internal'].append(sender_is_internal)

            recipients = Preprocessing.parse_to_emails(message)
            cc_recipients =  Preprocessing.parse_cc_emails(message)
            d['recipients'].append(recipients if recipients else 'UNKNOWN')
            d['recipients_cc'].append(cc_recipients if cc_recipients else 'UNKNOWN')
            recipients_amount = Preprocessing.count_emails(recipients) + Preprocessing.count_emails(cc_recipients)
            d['recipients_amount'].append(recipients_amount)
            d['recipients_is_single'].append(recipients_amount == 1)

            recipients_domains = Preprocessing.get_recipient_domain(recipients) + Preprocessing.get_recipient_domain(cc_recipients)
            recipients_domains_unique = list(set(recipients_domains))
            d['recipients_domains'].append(recipients_domains_unique if recipients_domains_unique else 'UNKNOWN')
            d['recipients_domains_most_frequent'].append(max(set(recipients_domains), key=recipients_domains.count) if recipients_domains else 'UNKNOWN')
            recipients_is_internal = len([x for x in recipients_domains_unique if 'enron' in x]) == len(recipients_domains_unique)
            d['recipients_is_internal'].append(recipients_is_internal)

            d['email_is_internal'].append(sender_is_internal & recipients_is_internal)

            date = Preprocessing.parse_date(message)
            d['date'].append(date)
            year = Preprocessing.get_year(date)
            d['date_year'].append(year)
            month = Preprocessing.get_month(date)
            d['date_month'].append(month)
            day = Preprocessing.get_day(date)
            d['date_day'].append(day)
            hour = Preprocessing.get_hour(date)
            d['date_hour'].append(hour)
            weekday = Preprocessing.get_weekday(date)
            d['date_weekday'].append(weekday)
            d['sent_at_weekend'].append(Preprocessing.get_sent_at_weekend(date))
            d['sent_during_business_hours'].append(Preprocessing.get_sent_during_business_hours(date))

        return pd.DataFrame(data=d)

    @staticmethod
    def remove_html_tags(text, check_for_tags=True):
        """Convert html to corresponding md."""
        if (not check_for_tags) or (check_for_tags and re.search(r'<[^>]+>', text) and re.search(r'</[^>]+>', text)):
            h = HTML2Text()
            h.ignore_links = True
            h.ignore_emphasis = True
            h.ignore_images = True
            return h.handle(text)
        else:
            return text

    @staticmethod
    def parse_body(message):
        def decode_part(text, encoding=None):
            if encoding is None:
                encoding = 'utf-8'

            try:
                text = text.decode(encoding, 'replace')
            except LookupError:
                text = text.decode('utf-8', 'replace')

            return text

        if message.is_multipart():
            body_text = ''
            for part in message.walk():
                ctype = part.get_conent_type()
                ccharset = part.get_content_charset()
                cdispo = str(part.get('Content-Disposition'))

                if (ctype == 'text/plain' or ctype == 'text/html') and 'attachment' not in cdispo:
                    text = part.get_payload(decode=True)
                    body_text = decode_part(text, encoding=ccharset)
                    break
            return body_text
        else:
            return decode_part(message.get_payload(decode=True))

    @staticmethod
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

    @staticmethod
    def clean_text(text):
        return preprocess_text(text, fix_unicode=True, lowercase=True, transliterate=True,
                        no_urls=True, no_emails=True, no_phone_numbers=True,
                        no_numbers=True, no_currency_symbols=True, no_punct=True,
                        no_contractions=True, no_accents=True)

    @staticmethod
    def parse_from_email(message):
        return utils.parseaddr(message.get('from'))[1]

    @staticmethod
    def parse_to_emails(message):
        return ','.join([elem[1] for elem in utils.getaddresses(message.get_all('to', []))])

    @staticmethod
    def parse_cc_emails(message):
        return ','.join([elem[1] for elem in utils.getaddresses(message.get_all('cc', []))])

    @staticmethod
    def parse_date(message):
        date = message.get('date', '')
        date = re.sub(r'\(\w+\)', '', date).strip(whitespace)
        try:
            return dateparser.parse(date)
        except Exception:
            return 0

    @staticmethod
    def get_email_domain(email):
        if email and '@' in email:
            return email.split('@', 1)[1]
        else:
            return 'UNKNOWN'

    @staticmethod
    def get_recipient_domain(emails):
        if emails:
            recipients = []
            for email in emails.split(','):
                recipients.append(Preprocessing.get_email_domain(email))
            return recipients
        else:
            return []

    @staticmethod
    def count_emails(emails):
        if emails:
            return len(emails.split(','))
        else:
            return 0

    @staticmethod
    def get_year(date):
        try:
            return int(date.strftime('%y'))
        except Exception:
            return -1

    @staticmethod
    def get_month(date):
        try:
            return int(date.strftime('%m'))
        except Exception:
            return -1

    @staticmethod
    def get_day(date):
        try:
            return int(date.strftime('%d'))
        except Exception:
            return -1

    @staticmethod
    def get_hour(date):
        try:
            return int(date.strftime('%-H'))
        except Exception:
            return -1

    @staticmethod
    def get_weekday(date):
        try:
            # 0 is sunday and 6 is saturday
            return int(date.strftime('%w'))
        except Exception:
            return -1

    @staticmethod
    def get_sent_at_weekend(date):
        try:
            # 0 is sunday and 6 is saturday
            weekday = get_weekday(date)
            return (weekday == 0 or weekday == 6)
        except Exception:
            return False

    @staticmethod
    def get_sent_during_business_hours(date):
        try:
            # assuming enron staff had 7 to 7 jobs and no work on sundays ;-)
            hour = Preprocessing.get_hour(date)
            weekday = int(date.strftime('%w'))
            is_business_hour = (hour >= 7 and hour <= 19 and weekday != 0 and weekday != 6)
            return is_business_hour
        except:
            return False

    @staticmethod
    def remove_nan_drop(data, column):
    	# TODO: replace with better strategy for handling nan values (maybe different strategies for different columns)
    	data = data.loc[pd.notnull(data[column]), :]
    	data = data.reset_index(drop=True)
    	return data

class FolderSplitting:
    @staticmethod
    def run(df, size):
        folder_names = np.unique(df.folder_name)
        df['folder_chunk'] = 0
        for folder in folder_names:
            files_for_folder = df.loc[df.folder_name == folder]
            amount_of_chunks = int(files_for_folder.shape[0] / size)
            for i in range(0, amount_of_chunks):
                selection = df[(df.folder_name == folder) & (df.folder_chunk == 0)]
                if (i == amount_of_chunks - 1):
                    indeces = selection.index
                else:
                    indeces = selection.sample(size).index
                for index in indeces:
                    df.loc[df.index == index, 'folder_chunk'] = i + 1

class FolderAggregation:
    @staticmethod
    def aggregate_by_folder_name(df):
        folder_names = np.unique(df.folder_name)
        columns = [col for col in df.columns if 'feat_' in col]
        columns.append('folder_name')
        df_avg = pd.DataFrame(columns=columns)
        for folder in folder_names:
            row = []
            folder_df = df[df.folder_name == folder]
            for column in folder_df:
                if 'feat_' in column:
                    row.append(folder_df[column].mean())
                elif 'folder_name' in column:
                    print(folder_df[column].values[0])
                    row.append(folder_df[column].values[0])

            df_avg = df_avg.append(pd.DataFrame([row], columns=df_avg.columns), ignore_index=True)
        return df_avg

    @staticmethod
    def aggregate_by_folder_name_and_chunk(df):
        folder_names = np.unique(df.folder_name)
        columns = [col for col in df.columns if 'feat_' in col]
        columns.append('folder_name')
        df_avg = pd.DataFrame(columns=columns)
        for folder in folder_names:
            folder_chunks = np.unique(df[df.folder_name == folder].folder_chunk)
            for chunk in folder_chunks:
                row = []
                folder_df = df.loc[(df.folder_name == folder) & (df.folder_chunk == chunk)]
                for column in folder_df:
                    if 'feat_' in column:
                        row.append(folder_df[column].mean())
                    elif 'folder_name' in column:
                        print(folder + str(chunk))
                        row.append(folder + str(chunk))

                df_avg = df_avg.append(pd.DataFrame([row], columns=df_avg.columns), ignore_index=True)
        return df_avg

class Features:
    @staticmethod
    def get_vectorizer(max_features, use_porter=False, norm='l2'):
        def get_porter_tfidf_vectorizer():
            stemmer = PorterStemmer()
            analyzer = TfidfVectorizer().build_analyzer()
            def stemmed_words(doc):
                return (stemmer.stem(w) for w in analyzer(doc))
            return TfidfVectorizer(strip_accents='ascii', min_df=0.7, max_df=5, analyzer=stemmed_words)

        if (use_porter):
            return get_porter_tfidf_vectorizer()
        else:
            return TfidfVectorizer(
                strip_accents='ascii',
                analyzer='word',
                stop_words='english',
                max_df=0.7,
                min_df=5,
                max_features=max_features,
                norm=norm
            )

    @staticmethod
    def vectorize(vectorizer, df, column, prefix):
        return pd.DataFrame(vectorizer.fit_transform(df[column]).todense()).add_prefix(prefix)

    @staticmethod
    def add_features(df):
        d = {}
        boolean_features = [
            'sender_is_internal',
            'recipients_is_single',
            'recipients_is_internal',
            'email_is_thread',
            'sent_at_weekend',
            'sent_during_business_hours'
        ]
        for feat in boolean_features:
            d['feat_' + feat] = df[feat].apply(lambda x: int(x))

        d = Features.add_quarter_feature(d, df)
        d = Features.add_weekday_feature(d, df)
        d = Features.add_sent_between_feature(d, df)
        d = Features.add_body_length_feature(d, df)
        d = Features.add_recipients_amount_feature(d, df)

        return pd.DataFrame(data=d)
        # Feature Ideas:
        # subject longer than body

    @staticmethod
    def add_quarter_feature(d, df):
        d['feat_sent_quarter_1'] = ((1 <= df.date_month) & (df.date_month <= 3)).apply(lambda x: int(x))
        d['feat_sent_quarter_2'] = ((4 <= df.date_month) & (df.date_month <= 6)).apply(lambda x: int(x))
        d['feat_sent_quarter_3'] = ((7 <= df.date_month) & (df.date_month <= 9)).apply(lambda x: int(x))
        d['feat_sent_quarter_4'] = ((10 <= df.date_month) & (df.date_month <= 12)).apply(lambda x: int(x))
        return d

    @staticmethod
    def add_weekday_feature(d, df):
        d['feat_weekday_0'] = (df.date_weekday == 0).apply(lambda x: int(x))
        d['feat_weekday_1'] = (df.date_weekday == 1).apply(lambda x: int(x))
        d['feat_weekday_2'] = (df.date_weekday == 2).apply(lambda x: int(x))
        d['feat_weekday_3'] = (df.date_weekday == 3).apply(lambda x: int(x))
        d['feat_weekday_4'] = (df.date_weekday == 4).apply(lambda x: int(x))
        d['feat_weekday_5'] = (df.date_weekday == 5).apply(lambda x: int(x))
        d['feat_weekday_6'] = (df.date_weekday == 6).apply(lambda x: int(x))
        return d

    @staticmethod
    def add_sent_month_feature(d, df):
        d['feat_sent_month_1'] = ((1 <= df.date_day) & (df.date_day <= 3)).apply(lambda x: int(x)) # month beginning
        d['feat_sent_month_2'] = ((4 <= df.date_day) & (df.date_day <= 27)).apply(lambda x: int(x)) # month during
        d['feat_sent_month_3'] = (28 <= df.date_day).apply(lambda x: int(x)) # month end
        return d

    @staticmethod
    def add_sent_between_feature(d, df):
        def sent_between(hour, min, max):
            try:
                return int(min <= hour < max)
            except:
                return -1
        d['feat_sent_between_1'] = df.date_hour.apply(lambda x: sent_between(x, 0, 3))
        d['feat_sent_between_2'] = df.date_hour.apply(lambda x: sent_between(x, 3, 6))
        d['feat_sent_between_3'] = df.date_hour.apply(lambda x: sent_between(x, 6, 9))
        d['feat_sent_between_4'] = df.date_hour.apply(lambda x: sent_between(x, 9, 12))
        d['feat_sent_between_5'] = df.date_hour.apply(lambda x: sent_between(x, 12, 15))
        d['feat_sent_between_6'] = df.date_hour.apply(lambda x: sent_between(x, 15, 18))
        d['feat_sent_between_7'] = df.date_hour.apply(lambda x: sent_between(x, 18, 21))
        d['feat_sent_between_8'] = df.date_hour.apply(lambda x: sent_between(x, 21, 24))
        return d

    @staticmethod
    def add_body_length_feature(d, df):
        d['feat_body_length_1'] = ((0 <= df.body_length) & (df.body_length <= 10)).apply(lambda x: int(x))
        d['feat_body_length_2'] = ((10 < df.body_length) & (df.body_length <= 200)).apply(lambda x: int(x))
        d['feat_body_length_3'] = ((200 < df.body_length) & (df.body_length <= 1000)).apply(lambda x: int(x))
        d['feat_body_length_4'] = (1000 < df.body_length).apply(lambda x: int(x))
        return d

    @staticmethod
    def add_recipients_amount_feature(d, df):
        d['feat_recipients_amount_1'] = ((0 <= df.recipients_amount) & (df.recipients_amount <= 1)).apply(lambda x: int(x))
        d['feat_recipients_amount_2'] = ((1 < df.recipients_amount) & (df.recipients_amount <= 10)).apply(lambda x: int(x))
        d['feat_recipients_amount_3'] = (10 < df.recipients_amount).apply(lambda x: int(x))
        return d

    @staticmethod
    def get_df_columns(df, feat_prefix):
        return [col for col in df if col.startswith(feat_prefix)]

    @staticmethod
    def drop(df, feat_prefix):
        columns = Features.get_df_columns(df, feat_prefix)
        return df.drop(columns, inplace=False, axis=1)

    @staticmethod
    def reduce_features(df, feat_prefix, n_components):
        columns = Features.get_df_columns(df, feat_prefix)
        reduced = PCA(n_components=n_components).fit_transform(df[columns])
        df_dropped = df.drop(columns, inplace=False, axis=1)
        new_feat_df = pd.DataFrame(reduced).add_prefix(feat_prefix)
        return pd.concat([df_dropped, new_feat_df], axis=1)


class EmailClusteringTool:

    def __init__(self, classifier, vectorizer_body, vectorizer_subject):
        super().__init__()
        self.classifier = classifier
        self.vectorizer_body = vectorizer_body
        self.vectorizer_subject = vectorizer_subject

    def create_df(self, text):
        return pd.DataFrame(data=[[text]], columns=['raw'])

    def preprocess(self, df):
        return Preprocessing.preprocess_emails(df)

    def prepare_email(self, text):
        df = self.create_df(text)
        df = self.preprocess(df)
        df = pd.concat([
            Features.vectorize(self.vectorizer_subject, df, 'subject_clean', 'feat_subject_'),
            Features.vectorize(self.vectorizer_body, df, 'body_clean', 'feat_body_'),
            Features.add_features(df)
        ], axis=1)
        return df

    def predict_cluster(self, text):
        df = self.prepare_email(text)
        return self.classifier.predict(df)[0]

