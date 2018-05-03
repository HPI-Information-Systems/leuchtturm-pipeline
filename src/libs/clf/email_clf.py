"""Classify emails."""

import pickle


with open('./models/email_clf.pickle', 'rb') as f:
    clf = pickle.load(f)


def get_email_category(text):
    """Get top category."""
    return clf.predict_top_category(text)


def get_category_prob(text):
    """Get dict containing class labels and confidence."""
    return clf.predict_proba(text)
