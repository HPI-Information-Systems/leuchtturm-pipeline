"""Phrase Detection pipes for leuchtturm pipelines."""

import ujson as json
import itertools
import nltk
import string
import gensim
from textacy import keyterms, Doc
from textacy.preprocess import preprocess_text, replace_urls, replace_emails, replace_phone_numbers, replace_numbers

from .common import Pipe


def extract_candidate_chunks(text, grammar=r'KT: {(<JJ>* <NN.*>+ <IN>)? <JJ>* <NN.*>+}'):
    # exclude candidates that are stop words or entirely punctuation
    punct = set(string.punctuation)
    stop_words = set(nltk.corpus.stopwords.words('english'))
    # tokenize, POS-tag, and chunk using regular expressions
    chunker = nltk.chunk.regexp.RegexpParser(grammar)
    tagged_sents = nltk.pos_tag_sents(nltk.word_tokenize(sent) for sent in nltk.sent_tokenize(text))
    all_chunks = list(itertools.chain.from_iterable(nltk.chunk.tree2conlltags(chunker.parse(tagged_sent))
                                                    for tagged_sent in tagged_sents))

    def lambda_unpack(f):
        return lambda args: f(*args)

    # join constituent chunk words into a single chunked phrase
    candidates = [' '.join(word for word, pos, chunk in group).lower() for key, group in
                  itertools.groupby(all_chunks, lambda_unpack(lambda word, pos, chunk: chunk != 'O')) if key]

    return [cand for cand in candidates
            if cand not in stop_words and not all(char in punct for char in cand)]


def score_keyphrases_by_tfidf(texts):
    # extract candidates from each text in texts, either chunks or words
    boc_texts = [extract_candidate_chunks(text) for text in texts]

    print(boc_texts)

    # make gensim dictionary and corpus
    dictionary = gensim.corpora.Dictionary(boc_texts)
    corpus = [dictionary.doc2bow(boc_text) for boc_text in boc_texts]
    # transform corpus with tf*idf model
    tfidf = gensim.models.TfidfModel(corpus)
    corpus_tfidf = tfidf[corpus]

    for document in corpus_tfidf:
        print(document)

    for document in dictionary:
        print(document)

    return corpus_tfidf, dictionary


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

        corpus_cleaned = preprocess_text(
            corpus_joined,
            no_currency_symbols=True,
            no_contractions=True,
            no_accents=True,
            no_punct=True
        )
        for func in [replace_urls, replace_emails, replace_phone_numbers, replace_numbers]:
            corpus_cleaned = func(corpus_cleaned, replace_with='')

        keyphrases = keyterms.sgrank(Doc(corpus_joined, lang='en'), n_keyterms=100)

        rdd = rdd.map(lambda document: self.run_on_document(document, keyphrases))

        return rdd
