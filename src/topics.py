"""Topic modeling pipes for leuchtturm pipelines."""

from collections import defaultdict
import ujson as json
import pickle
from string import punctuation

from gensim import corpora, models
from nltk.corpus import stopwords as nltksw
from nltk.stem.wordnet import WordNetLemmatizer


def topic_model_training(rdd):
    """Train topic model and export it.

    Train a lda topic model using gensim.
    Export pickeled model to a textfile.
    """
    iterations = 1000
    num_topics = 100
    alpha = 50 / num_topics
    eta = 0.1
    raw_corpus = rdd.collect()

    stopwords = nltksw.words('english')

    lemma = WordNetLemmatizer()
    short_tokens = set()
    numbers = set()

    def clean(doc):
        tokens = [token for token in doc.lower().split()]
        punc_free = [token.strip(punctuation) for token in tokens]
        empty_string_free = [token for token in punc_free if token]
        stopword_free = [word for word in empty_string_free if word not in stopwords]
        short_token_free = [word if len(word) > 2 else short_tokens.add(word) for word in stopword_free]
        empty_string_free2 = [token for token in short_token_free if token]
        numerics_free = []
        for token in empty_string_free2:
            if [char for char in token if not (char.isdigit() or char in punctuation)]:
                numerics_free.append(token)
            else:
                numerics_free.append('lt_number')
                numbers.add(token)
        lemmatized = [lemma.lemmatize(word) for word in numerics_free]
        return lemmatized

    docs = [clean(doc) for doc in raw_corpus]

    word_doc_appearances = defaultdict(set)
    for i, doc in enumerate(docs):
        for token in doc:
            word_doc_appearances[token].add(i)

    high_freq_tokens = set()
    low_freq_tokens = set()

    MIN_FREQ = 3
    MAX_PERCENTAGE = 0.05
    max_freq = MAX_PERCENTAGE * len(docs)

    def filter_by_freq(doc):
        filtered_doc = []
        for token in doc:
            if token == 'lt_number':
                filtered_doc.append(token)
            elif len(word_doc_appearances[token]) < MIN_FREQ:
                low_freq_tokens.add(token)
            elif len(word_doc_appearances[token]) > max_freq:
                high_freq_tokens.add(token)
            else:
                filtered_doc.append(token)
        return filtered_doc

    docs = [filter_by_freq(doc) for doc in docs]

    docs = [doc for doc in docs if doc]

    processed_corpus = docs

    dictionary = corpora.Dictionary(processed_corpus)
    with open('./models/pickled_lda_dictionary.p', 'wb') as pfile:
        pickle.dump(dictionary, pfile)

    bow_corpus = [dictionary.doc2bow(text) for text in processed_corpus]

    lda = models.ldamodel.LdaModel(bow_corpus, num_topics=num_topics, iterations=iterations, eta=eta, alpha=alpha)
    with open('./models/pickled_lda_model.p', 'wb') as pfile:
        pickle.dump(lda, pfile)

    return rdd


def topic_model_prediction(rdd, read_from='text_clean', path_model='./models/pickled_lda_model.p',
                           path_dict='./models/pickled_lda_dictionary.p'):
    """Predict topics for a given text.

    Needs trained lda model + dictionary.
    Will add topic field.
    """
    def run_on_partition(partition):
        """Run task in spark context. Partitionwise for performance reasosn."""
        with open(path_model, mode='rb') as pfile:
            model = pickle.load(pfile)

        with open(path_dict, mode='rb') as pfile:
            dictionary = pickle.load(pfile)

        def get_topics(text):
            """Predict topics for a text."""
            bow = dictionary.doc2bow(text.split())

            topic_terms = []
            for topic in model.get_document_topics(bow):
                terms = map(lambda xy: (dictionary[xy[0]], xy[1]), model.get_topic_terms(topic[0], topn=10))
                # TODO Fix formatting
                topic_terms.append(str((str(topic[1]), (list(terms)))))

            return topic_terms

        def run_on_document(raw_message):
            """Predict topics for a leuchtturm document."""
            document = json.loads(raw_message)
            # TODO srsly, this way the resulting 'list' isn't even a list...
            document['topics'] = str(get_topics(document[read_from]))

            return json.dumps(document)

        for doc in partition:
            yield run_on_document(doc)

    return rdd.mapPartitions(lambda x: run_on_partition(x))
