"""Modified SGRank for extractin keyphrases from multiple documents."""

from __future__ import absolute_import, division, print_function, unicode_literals

from collections import Counter, defaultdict
import itertools
import logging
import math
from operator import itemgetter
from cytoolz import itertoolz
import networkx as nx
from textacy import extract

LOGGER = logging.getLogger(__name__)


def sgrank_for_multiple_documents(doc, ngrams=(1, 2, 3, 4, 5, 6), normalize='lemma', window_width=1500,
                                  n_keyterms=10, idf=None):
    """Extract key terms from a document using the [SGRank]_ algorithm."""
    n_toks = len(doc)
    if isinstance(n_keyterms, float):
        if not 0.0 < n_keyterms <= 1.0:
            raise ValueError('`n_keyterms` must be an int, or a float between 0.0 and 1.0')
        n_keyterms = int(round(n_toks * n_keyterms))
    if window_width < 2:
        raise ValueError('`window_width` must be >= 2')
    window_width = min(n_toks, window_width)
    min_term_freq = min(n_toks // 1000, 4)
    if isinstance(ngrams, int):
        ngrams = (ngrams,)

    # build full list of candidate terms
    # if inverse doc freqs available, include nouns, adjectives, and verbs;
    # otherwise, just include nouns and adjectives
    # (without IDF downweighting, verbs dominate the results in a bad way)
    include_pos = {'NOUN', 'PROPN', 'ADJ', 'VERB'} if idf else {'NOUN', 'PROPN', 'ADJ'}
    terms = itertoolz.concat(
        extract.ngrams(doc, n, filter_stops=True, filter_punct=True, filter_nums=False,
                       include_pos=include_pos, min_freq=min_term_freq)
        for n in ngrams)

    # get normalized term strings, as desired
    # paired with positional index in document and length in a 3-tuple
    if normalize == 'lemma':
        terms = [(term.lemma_, term.start, len(term)) for term in terms if len(term.lemma_) > 6]
    elif normalize == 'lower':
        terms = [(term.orth_.lower(), term.start, len(term)) for term in terms]
    elif not normalize:
        terms = [(term.text, term.start, len(term)) for term in terms]
    else:
        terms = [(normalize(term), term.start, len(term)) for term in terms]

    # pre-filter terms to the top N ranked by TF or modified TF*IDF
    n_prefilter_kts = max(3 * n_keyterms, 100)
    term_text_counts = Counter(term[0] for term in terms)
    if idf:
        mod_tfidfs = {
            term: count * idf.get(term, 1) if ' ' not in term else count
            for term, count in term_text_counts.items()}
        terms_set = {
            term for term, _
            in sorted(mod_tfidfs.items(), key=itemgetter(1), reverse=True)[:n_prefilter_kts]}
    else:
        terms_set = {term for term, _ in term_text_counts.most_common(n_prefilter_kts)}
    terms = [term for term in terms if term[0] in terms_set]

    # compute term weights from statistical attributes:
    # not subsumed frequency, position of first occurrence, and num words
    term_weights = {}
    seen_terms = set()
    for term in terms:
        term_text = term[0]
        # we only want the *first* occurrence of a unique term (by its text)
        if term_text in seen_terms:
            continue
        seen_terms.add(term_text)
        # TODO: assess how best to scale term len
        term_len = math.sqrt(term[2])  # term[2]
        term_count = term_text_counts[term_text]
        subsum_count = sum(term_text_counts[t2] for t2 in terms_set
                           if t2 != term_text and term_text in t2)
        term_freq_factor = term_count - subsum_count
        if idf and term[2] == 1:
            term_freq_factor *= idf.get(term_text, 1)
        term_weights[term_text] = term_freq_factor * term_len

    # filter terms to only those with positive weights
    terms = [term for term in terms if term_weights[term[0]] > 0]

    n_coocs = defaultdict(lambda: defaultdict(int))
    sum_logdists = defaultdict(lambda: defaultdict(float))

    # iterate over windows
    log_ = math.log  # localize this, for performance
    for start_ind in range(n_toks):
        end_ind = start_ind + window_width
        window_terms = (term for term in terms
                        if start_ind <= term[1] <= end_ind)
        # get all token combinations within window
        for t1, t2 in itertools.combinations(window_terms, 2):
            n_coocs[t1[0]][t2[0]] += 1
            sum_logdists[t1[0]][t2[0]] += log_(window_width / max(abs(t1[1] - t2[1]), 1))
        if end_ind > n_toks:
            break

    # compute edge weights between co-occurring terms (nodes)
    edge_weights = defaultdict(lambda: defaultdict(float))
    for t1, t2s in sum_logdists.items():
        for t2 in t2s:
            edge_weights[t1][t2] =\
                ((1.0 + sum_logdists[t1][t2]) / n_coocs[t1][t2]) * term_weights[t1] * term_weights[t2]
    # normalize edge weights by sum of outgoing edge weights per term (node)
    norm_edge_weights = []
    for t1, t2s in edge_weights.items():
        sum_edge_weights = sum(t2s.values())
        norm_edge_weights.extend((t1, t2, {'weight': weight / sum_edge_weights})
                                 for t2, weight in t2s.items())

    # build the weighted directed graph from edges, rank nodes by pagerank
    graph = nx.DiGraph()
    graph.add_edges_from(norm_edge_weights)
    term_ranks = nx.pagerank_scipy(graph)

    return sorted(term_ranks.items(), key=itemgetter(1, 0), reverse=True)[:n_keyterms]
