"""Phrase Detection pipes for leuchtturm pipelines."""

import ujson as json
from textacy import keyterms, Doc
from textacy.preprocess import preprocess_text, replace_urls, replace_emails, replace_phone_numbers, replace_numbers

from .common import Pipe


class PhraseDetection(Pipe):
    """Phrase Detection on emails."""

    def __init__(self, conf, read_from='text_clean'):
        """Set params. read_from: field to search entities in."""
        super().__init__(conf)
        self.read_from = read_from

    def run_on_document(self, raw_message, keyphrases):
        """Get top phrases for a leuchtturm document."""
        document = json.loads(raw_message)

        document['top_phrases'] = []
        for phrase in keyphrases:
            if phrase[0].lower() in document[self.read_from].lower():
                document['top_phrases'].append(phrase)

        return json.dumps(document, ensure_ascii=False)

    def run(self, rdd):
        """Run task in a spark context."""
        corpus = rdd.map(lambda document: json.loads(document)[self.read_from]).collect()

        corpus_joined = '\n\n\n'.join(corpus)

        corpus_joined = corpus_joined[:999999]

        corpus_cleaned = preprocess_text(
            corpus_joined,
            no_currency_symbols=True,
            no_contractions=True,
            no_accents=True,
            no_punct=True
        )
        for func in [replace_urls, replace_emails, replace_phone_numbers, replace_numbers]:
            corpus_cleaned = func(corpus_cleaned, replace_with='')

        keyphrases = keyterms.sgrank(Doc(corpus_joined, lang='en_core_web_sm'), n_keyterms=100)

        rdd = rdd.map(lambda document: self.run_on_document(document, keyphrases))

        return rdd
