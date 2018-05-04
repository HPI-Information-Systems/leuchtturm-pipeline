"""Make classification steps available for pipeline."""

from .clf_features import *
from .clf_preprocessing import *
import pandas as pd


class EmailClfInterface(object):

    def __init__(self, classifier, vectorizer_body, vectorizer_subject, label_encoder):
        super().__init__()
        self.classifier = classifier
        self.vectorizer_body = vectorizer_body
        self.vectorizer_subject = vectorizer_subject
        self.label_encoder = label_encoder

    def create_df(self, text):
        return pd.DataFrame(data=[[text]], columns=['email'])

    def preprocess(self, df):
        return parse_raw_emails(df)

    def add_features(self, df):
        return add_all_features(df)

    def transform(self, df):
        return transform_features(df)

    def vectorize_field(self, df, field, new_field, vectorizer):
        return add_vectorized_field(df, field, new_field, vectorizer)

    def drop_features(self, df):
        return drop_features(df)

    def prepare_email(self, text):
        df = self.create_df(text)
        df = self.preprocess(df)
        df = self.add_features(df)
        df = self.transform(df)
        df = self.vectorize_field(df, 'body', 'b', self.vectorizer_body)
        df = self.vectorize_field(df, 'subject', 's', self.vectorizer_subject)
        df = self.drop_features(df)

        return df

    def classify_email(self, df):
        return self.classifier.predict(df)[0]

    def classify_email_proba(self, df):
        return self.classifier.predict_proba(df)[0]

    def translate_label(self, number):
        return self.label_encoder.inverse_transform(number)

    def predict_top_category(self, text):
        df = self.prepare_email(text)
        label = self.classify_email(df)

        return self.translate_label(label)

    def predict_proba(self, text):
        df = self.prepare_email(text)
        probabilities = self.classify_email_proba(df)
        labels = self.label_encoder.classes_

        return dict(zip(labels, probabilities))
