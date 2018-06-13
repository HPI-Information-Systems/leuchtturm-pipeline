"""Phrase Detection pipes for leuchtturm pipelines."""

import ujson as json
import itertools
import nltk
import string
import gensim
import textacy
from textacy import keyterms

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

    def run_on_document(self, raw_message):
        """Get top phrases for a leuchtturm document."""
        document = json.loads(raw_message)
        document['top_phrases'] = keyterms.sgrank(textacy.Doc(document[self.read_from], lang='en'))

        print(document['top_phrases'])

        return json.dumps(document, ensure_ascii=False)

    def run(self, rdd):
        """Run task in a spark context."""
        corpus = rdd.map(lambda document: json.loads(document)[self.read_from]).collect()

        corpus_joined = '. '.join(corpus)

        keyphrases = keyterms.sgrank(textacy.Doc(corpus_joined, lang='en'))

        print(keyphrases)

        return rdd.map(lambda document: self.run_on_document(document))
