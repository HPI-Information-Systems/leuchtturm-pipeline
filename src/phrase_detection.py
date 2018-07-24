"""Phrase Detection pipes for leuchtturm pipelines."""

import ujson as json
from textacy import keyterms, Doc
from textacy.preprocess \
    import preprocess_text, replace_urls, replace_emails, replace_phone_numbers, replace_numbers, remove_punct
from src.common import SparkProvider
from ast import literal_eval
import gensim
import itertools
import nltk
import string
from src.sgrank import sgrank_for_multiple_documents

from .common import Pipe


def clean_text(text):
    """Clean the input text."""
    for func in [replace_urls, replace_emails, replace_phone_numbers, replace_numbers]:
        text = func(text, replace_with='')

    text = preprocess_text(
        text,
        no_currency_symbols=True,
        no_contractions=True,
        no_accents=True
    )
    return remove_punct(text, marks='|')


def extract_candidate_phrases(text):
    """Extract candidate phrases for the text."""
    text_cleaned = clean_text(text)
    # tokenize, POS-tag, and split using regular expressions
    chunker = nltk.chunk.regexp.RegexpParser(r'KT: {(<JJ>* <NN.*>+ <IN>)? <JJ>* <NN.*>+}')
    tagged_sents = nltk.pos_tag_sents(nltk.word_tokenize(sent) for sent in nltk.sent_tokenize(text_cleaned))
    all_phrases = list(itertools.chain.from_iterable(nltk.chunk.tree2conlltags(chunker.parse(tagged_sent))
                                                     for tagged_sent in tagged_sents))

    def lambda_unpack(f):
        return lambda args: f(*args)

    # join constituent words into a single phrase
    candidates = [' '.join(word for word, pos, chunk in group).lower() for key, group in
                  itertools.groupby(all_phrases, lambda_unpack(lambda word, pos, chunk: chunk != 'O')) if key]

    # exclude candidates that are stop words or entirely punctuation
    punct = set(string.punctuation)
    stop_words = set(nltk.corpus.stopwords.words('english'))

    return [cand for cand in candidates if cand not in stop_words and not all(char in punct for char in cand)]


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

        text = clean_text(document['header']['subject'] + '. ' + document[self.read_from])
        document['keyphrases_single'] = keyterms.sgrank(
            Doc(text, lang='en_core_web_sm'),
            ngrams=literal_eval(self.conf.get('phrase_detection', 'length')),
            n_keyterms=10,
            window_width=self.conf.get('phrase_detection', 'window_width')
        )

        common_words = literal_eval(self.conf.get('phrase_detection', 'common_words'))

        document['keyphrases_multiple'] = []
        document['keyphrases_multiple_co'] = []
        document['keyphrases_multiple_filtered'] = []
        document['keyphrases_multiple_filtered_co'] = []
        for phrase in keyphrases:
            if phrase[0] in document[self.read_from].lower():
                document['keyphrases_multiple'].append(phrase[0])
                document['keyphrases_multiple_co'].append(phrase)
                if phrase[0] not in common_words:
                    document['keyphrases_multiple_filtered'].append(phrase[0])
                    document['keyphrases_multiple_filtered_co'].append(phrase)

        return json.dumps(document, ensure_ascii=False)

    def get_keyphrases_for_text(self, text, n_keyterms):
        """Get keyphrases for a text."""
        text = clean_text(text)

        return sgrank_for_multiple_documents(
            Doc(text, lang='en_core_web_sm'),
            ngrams=literal_eval(self.conf.get('phrase_detection', 'length')),
            n_keyterms=n_keyterms,
            window_width=self.conf.get('phrase_detection', 'window_width')
        )

    def add_keyphrases_by_tfidf(self, documents):
        """Add keyphrases by tfidf."""
        documents = documents.map(json.loads)

        def add_boc_text(document):
            text = document['header']['subject'] + '. ' + document[self.read_from]
            cands = extract_candidate_phrases(text)
            document['phrases_boc_text'] = cands
            return document

        documents = documents.map(add_boc_text)

        boc_texts = documents.map(lambda document: document['phrases_boc_text']).collect()
        gen_dictionary = gensim.corpora.Dictionary(boc_texts)

        def add_bow(document, dictionary):
            document['phrases_bow'] = dictionary.doc2bow(document.pop('phrases_boc_text', []))
            return document

        documents = documents.map(lambda document: add_bow(document, gen_dictionary))

        corpus = documents.map(lambda document: document['phrases_bow']).collect()
        tfidf = gensim.models.TfidfModel(corpus)

        def add_tfidf(document, dictionary, tfidf_model):
            phrases = tfidf_model[document.pop('phrases_bow', [])]
            document['keyphrases_tfidf'] = []
            document['keyphrases_tfidf_co'] = []
            phrases.sort(key=lambda tup: tup[1], reverse=True)
            for phrase in phrases:
                document['keyphrases_tfidf'].append(dictionary.get(phrase[0]))
                document['keyphrases_tfidf_co'].append([dictionary.get(phrase[0]), phrase[1]])

            return document

        documents = documents.map(lambda document: add_tfidf(document, gen_dictionary, tfidf))

        return documents.map(lambda document: json.dumps(document, ensure_ascii=False))

    def run(self, rdd):
        """Run task in a spark context."""
        # rdd = self.add_keyphrases_by_tfidf(rdd)

        corpus = rdd.map(
            lambda document: json.loads(document)['header']['subject'] + '. ' + json.loads(document)[self.read_from]
        ).collect()
        corpus_joined = '. '.join(corpus)

        chunk_size = self.conf.get('phrase_detection', 'chunk_size')
        corpus_chunked = [corpus_joined[i:i + chunk_size] for i in range(0, len(corpus_joined), chunk_size)]

        sc = SparkProvider.spark_context(self.conf)
        chunked_rdd = sc.parallelize(corpus_chunked)
        phrases_rdd = chunked_rdd.flatMap(
            lambda chunk: self.get_keyphrases_for_text(chunk, self.conf.get('phrase_detection', 'amount'))
        )
        phrases_rdd = phrases_rdd.map(lambda phrase: (phrase[0].lower(), phrase[1]))
        phrases_rdd = phrases_rdd.aggregateByKey((0, 0), lambda a, b: (a[0] + b, a[1] + 1),
                                                 lambda a, b: (a[0] + b[0], a[1] + b[1]))
        phrases_list = phrases_rdd.mapValues(lambda v: v[0] / v[1]).sortBy(lambda x: x[1], False).collect()

        rdd = rdd.map(lambda document: self.run_on_document(document, phrases_list))

        return rdd
