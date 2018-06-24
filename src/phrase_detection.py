"""Phrase Detection pipes for leuchtturm pipelines."""

import ujson as json
from textacy import keyterms, Doc
from textacy.preprocess import preprocess_text, replace_urls, replace_emails, replace_phone_numbers, replace_numbers
import math

from .common import Pipe


class PhraseDetection(Pipe):
    """Phrase Detection on emails."""

    def __init__(self, conf, read_from='text_clean'):
        """Set params. read_from: field to search entities in."""
        super().__init__(conf)
        self.read_from = read_from

    def run_on_document(self, raw_message, keyphrases):
        """Get keyphrases for a leuchtturm document."""
        document = json.loads(raw_message)

        document['top_phrases'] = []
        for phrase in keyphrases:
            if phrase[0].lower() in document[self.read_from].lower():
                document['top_phrases'].append(phrase)

        return json.dumps(document, ensure_ascii=False)

    def get_keyphrases_for_chunk(self, chunk):
        """Get keyphrases for a text chunk."""
        for func in [replace_urls, replace_emails, replace_phone_numbers, replace_numbers]:
            chunk = func(chunk, replace_with='')
        chunk = preprocess_text(
            chunk,
            no_currency_symbols=True,
            no_contractions=True,
            no_accents=True
        )
        return keyterms.sgrank(Doc(chunk, lang='en_core_web_sm'), ngrams=(2, 3, 4, 5, 6), n_keyterms=100)

    def run(self, rdd):
        """Run task in a spark context."""
        corpus = rdd.map(lambda document: json.loads(document)[self.read_from] + '.')

        length = corpus.map(lambda text: len(text)).sum()
        print(length)
        corpus = corpus.repartition(max(math.ceil(length / 500000), 1))

        print(corpus.collect())

        phrases_rdd = corpus.flatMap(lambda chunk: self.get_keyphrases_for_chunk(chunk))

        phrases_rdd = phrases_rdd.aggregateByKey((0, 0), lambda a, b: (a[0] + b, a[1] + 1),
                                                 lambda a, b: (a[0] + b[0], a[1] + b[1]))
        no_filter = phrases_rdd.filter(lambda v: v[1][1] > 0).mapValues(lambda v: v[0] / v[1]).sortBy(
            lambda x: x[1], False).collect()

        print(no_filter)

        rdd = rdd.map(lambda document: self.run_on_document(document, no_filter))

        return rdd
