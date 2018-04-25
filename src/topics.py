"""Topic modeling pipes for leuchtturm pipelines."""

from collections import defaultdict
import ujson as json
import pickle
from string import punctuation

from gensim import corpora, models
from nltk.corpus import stopwords as nltksw
from nltk.stem.wordnet import WordNetLemmatizer

from .common import Pipe


class TopicModelTraining(Pipe):
    """Train topic model and export it.

    Train a lda topic model using gensim.
    Export pickeled model to a textfile.
    """

    def __init__(self):
        """TODO: set params here (iterations, num_topics, ...)!! Especially output paths."""
        super().__init__()

    def run(self, rdd):
        """Run topic model training."""
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


class TopicModelPrediction(Pipe):
    """Predict topics for a given text.

    Needs trained lda model + dictionary.
    Will add topic field.
    """

    def __init__(self, read_from='text_clean', path_model='./models/pickled_lda_model.p',
                 path_dict='./models/pickled_lda_dictionary.p'):
        """Set params here."""
        super().__init__()
        self.read_from = read_from
        self.path_model = path_model
        self.path_dict = path_dict
        # TODO add variable train_topic_model=Bool to trigger tm training within the main pipeline

    def load_model(self):
        """Load lda model from defined path."""
        with open(self.path_model, mode='rb') as pfile:
            model = pickle.load(pfile)

        return model

    def load_dictionary(self):
        """Load dict for lda tm from defined path."""
        with open(self.path_dict, mode='rb') as pfile:
            dictionary = pickle.load(pfile)

        return dictionary

    def get_topics_for_doc(self, doc_id, text, model, dictionary):
        """Predict topics for a text."""
        def get_word_from_term_id_and_round(tuple):
            term = dictionary[tuple[0]]
            term_conf = round(float(tuple[1]), 8)
            return (term, term_conf)

        bow = dictionary.doc2bow(text.split())
        doc_topics = []

        for topic in model.get_document_topics(bow):
            topic_obj = {}
            topic_id = topic[0]
            term_id_conf_tuples = model.get_topic_terms(topic_id, topn=10)

            topic_obj['topic_id'] = topic_id
            topic_obj['topic_conf'] = round(float(topic[1]), 8)
            topic_obj['terms'] = list(map(get_word_from_term_id_and_round, term_id_conf_tuples))
            topic_obj['doc_id'] = doc_id

            doc_topics.append(topic_obj)

        return doc_topics

    def run_on_document(self, raw_message, model=None, dictionary=None):
        """Predict topics for a leuchtturm document."""
        model = model if model is not None else self.load_model()
        dictionary = dictionary if dictionary is not None else self.load_dictionary()

        document = json.loads(raw_message)
        doc_topics = self.get_topics_for_doc(document['doc_id'], document[self.read_from], model, dictionary)

        return [json.dumps(topic) for topic in doc_topics]

    def run_on_partition(self, partition):
        """Run task in spark context. Partitionwise for performance reasosn."""
        model = self.load_model()
        dictionary = self.load_dictionary()

        for doc in partition:
            yield self.run_on_document(doc, model=model, dictionary=dictionary)

    def run(self, rdd):
        """Run task in spark context."""
        return rdd.mapPartitions(lambda x: self.run_on_partition(x)) \
                  .flatMap(lambda x: x)
