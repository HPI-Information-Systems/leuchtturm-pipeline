"""Phrase Detection pipes for leuchtturm pipelines."""

import ujson as json
from textacy import keyterms, Doc
from textacy.preprocess \
    import preprocess_text, replace_urls, replace_emails, replace_phone_numbers, replace_numbers, remove_punct
from src.common import SparkProvider
from ast import literal_eval as make_tuple

from .common import Pipe


class PhraseDetection(Pipe):
    """Phrase Detection on emails."""

    def __init__(self, conf, read_from='text_clean'):
        """Set params. read_from: field to search entities in."""
        super().__init__(conf)
        self.read_from = read_from
        self.conf = conf

    def run_on_document(self, raw_message, keyphrases):
        """Get keyphrases for a leuchtturm document."""
        document = json.loads(raw_message)

        document['keyphrases'] = []
        for phrase in keyphrases:
            if phrase[0] in document[self.read_from].lower():
                document['keyphrases'].append(phrase)

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
        chunk = remove_punct(chunk, marks='|')

        return keyterms.sgrank(
            Doc(chunk, lang='en_core_web_sm'),
            ngrams=make_tuple(self.conf.get('phrase_detection', 'length')),
            n_keyterms=self.conf.get('phrase_detection', 'amount'),
            window_width=self.conf.get('phrase_detection', 'window_width')
        )

    def run(self, rdd):
        """Run task in a spark context."""
        corpus = rdd.map(lambda document: json.loads(document)[self.read_from])
        corpus_joined = '. '.join(corpus.collect())

        chunk_size = self.conf.get('phrase_detection', 'chunk_size')
        corpus_chunked = [corpus_joined[i:i + chunk_size] for i in range(0, len(corpus_joined), chunk_size)]

        sc = SparkProvider.spark_context(self.conf)
        chunked_rdd = sc.parallelize(corpus_chunked)
        phrases_rdd = chunked_rdd.flatMap(lambda chunk: self.get_keyphrases_for_chunk(chunk))
        phrases_rdd = phrases_rdd.map(lambda phrase: (phrase[0].lower(), phrase[1]))
        print(phrases_rdd)
        phrases_rdd = phrases_rdd.aggregateByKey((0, 0), lambda a, b: (a[0] + b, a[1] + 1),
                                                 lambda a, b: (a[0] + b[0], a[1] + b[1]))
        phrases_list = phrases_rdd.mapValues(lambda v: v[0] / v[1]).sortBy(lambda x: x[1], False).collect()

        rdd = rdd.map(lambda document: self.run_on_document(document, phrases_list))

        return rdd
