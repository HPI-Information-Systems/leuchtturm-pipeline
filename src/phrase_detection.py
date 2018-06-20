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

    def run_on_document(self, raw_message):
        """Get top phrases for a leuchtturm document."""
        document = json.loads(raw_message)

        text_cleaned = preprocess_text(
            document[self.read_from],
            no_currency_symbols=True,
            no_contractions=True,
            no_accents=True,
            no_punct=True
        )
        for func in [replace_urls, replace_emails, replace_phone_numbers, replace_numbers]:
            text_cleaned = func(text_cleaned, replace_with='')

        if len(text_cleaned) > 1000000:
            text_cleaned = text_cleaned[:1000000]

        document['keyphrases'] = keyterms.sgrank(Doc(text_cleaned, lang='en_core_web_sm'), n_keyterms=5)

        return json.dumps(document, ensure_ascii=False)

    def run(self, rdd):
        """Run task in a spark context."""
        rdd = rdd.map(lambda document: self.run_on_document(document))

        return rdd
