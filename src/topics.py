"""Topic modeling pipes for leuchtturm pipelines."""

from collections import defaultdict
import ujson as json
import pickle
from string import punctuation
import os

from gensim import corpora, models
from nltk.corpus import stopwords as nltksw
from nltk.stem.wordnet import WordNetLemmatizer

from .common import Pipe


class TopicModelTraining(Pipe):
    """Train topic model and export it.

    Train a lda topic model using gensim.
    Export pickeled model to a textfile.
    """

    def __init__(self, conf):
        """TODO: set params here (iterations, num_topics, ...)!! Especially output paths."""
        super().__init__(conf)
        self.conf = conf

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
        with open(self.conf.get('topic_modelling', 'file_dictionary'), 'wb') as pfile:
            pickle.dump(dictionary, pfile)

        bow_corpus = [dictionary.doc2bow(text) for text in processed_corpus]

        lda = models.ldamodel.LdaModel(bow_corpus, num_topics=num_topics, iterations=iterations, eta=eta, alpha=alpha)
        with open(self.conf.get('topic_modelling', 'file_model'), 'wb') as pfile:
            pickle.dump(lda, pfile)


class TopicModelPreprocessing(Pipe):
    """Preprocess documents into a bag-of-words so that it can be used to train a topic model."""

    def __init__(self, conf, read_from, write_to):
        """Set params."""
        super().__init__(conf)
        self.conf = conf
        self.read_from = read_from
        self.write_to = write_to
        self.stopwords = nltksw.words('english')
        self.lemma = WordNetLemmatizer()
        self.word_frequencies = defaultdict(lambda: 0)
        self.high_freq_words = set()
        self.low_freq_words = set()
        self.max_freq = -1

    def tokenize(self, body):
        """Split the body into words."""
        return body.lower().split()

    def remove_various_words(self, bow, sender, recipients):
        """Clean the bow from distracting words."""
        names = [part.lower() for part in sender['name'].split()]
        for recipient in recipients:
            for part in recipient['name'].split():
                names.append(part.lower())

        processed_bow = []
        for word in bow:
            # remove names of the sender and the recipients of the email
            word = word if word not in names else ''
            # remove punctuation characters
            word = word.strip(punctuation)
            # remove stopwords
            word = word if word not in self.stopwords else ''
            # remove very short words as they often don't carry any meaning
            word = word if len(word) > 2 else ''
            # remove numbers
            word = word if not self._is_numeric(word) else ''
            if word:
                processed_bow.append(word)

        return processed_bow

    def _is_numeric(self, word):
        for char in word:
            if not (char.isdigit() or char in punctuation):
                return False
        return True

    def lemmatize(self, bow):
        """Lemmatize each word in a bow."""
        return [self.lemma.lemmatize(word) for word in bow]

    def count_word_frequencies(self, bow):
        """Add the number of occurences of words in a bow to the global tracked word frequencies."""
        for word in bow:
            self.word_frequencies[word] += 1

    def run_on_document(self, item):
        """Run TM preprocessing on document."""
        document = json.loads(item)
        bow = self.tokenize(document[self.read_from])

        bow = self.remove_various_words(bow, document['header']['sender'], document['header']['recipients'])
        bow = self.lemmatize(bow)
        self.count_word_frequencies(bow)

        document[self.write_to] = bow
        return json.dumps(document)

    def filter_by_frequencies(self, item):
        """Remove all words from a bow that are too frequent or too infrequent in the whole corpus."""
        document = json.loads(item)
        bow = document[self.write_to]

        filtered_bow = []
        for word in bow:
            if self.word_frequencies[word] > self.conf.get('tm_preprocessing', 'min_freq') \
               and self.word_frequencies[word] < self.max_freq:
                filtered_bow.append(word)

        document[self.write_to] = filtered_bow
        return json.dumps(document)

    def run(self, rdd):
        """Run pipe in spark context."""
        self.max_freq = self.conf.get('tm_preprocessing', 'max_percentage') * rdd.count()
        rdd = rdd.map(self.run_on_document)
        return rdd.map(self.filter_by_frequencies)


class TopicModelBucketing(Pipe):
    """Bucket email documents by time slices."""

    def __init__(self, conf, read_from='bow'):
        """Set params."""
        super().__init__(conf)
        self.conf = conf
        self.read_from = read_from

    def remove_irrelevant_keys(self, item):
        """Remove all keys except for self.read_from (usually 'bow') as they're not needed for this task."""
        document = json.loads(item)
        date = document['header']['date']
        for key in list(set(document.keys()) - {self.read_from}):
            document.pop(key)
        document['date'] = date
        return json.dumps(document)

    def convert_to_tuple(self, item, bucket_timeframe=None):
        """Convert a document to a tuple of reduce-key and document."""
        document = json.loads(item)
        splitting_key = document['date']
        if not bucket_timeframe:
            bucket_timeframe = self.conf.get('tm_preprocessing', 'bucket_timeframe')

        if bucket_timeframe == 'year':
            # '2001-05-15T08:31:00Z' --> ['2001', '05-15T08:31:00Z'] --> '2001'
            splitting_key = document['date'].split('-', 1)[0]
        elif bucket_timeframe == 'month':
            # '2001-05-15T08:31:00Z' --> ['2001-05', '15T08:31:00Z'] --> '2001-05'
            splitting_key = document['date'].rsplit('-', 1)[0]
        elif bucket_timeframe == 'day':
            # '2001-05-15T08:31:00Z' --> ['2001-05-15', '08:31:00Z'] --> '2001-05-15'
            splitting_key = document['date'].split('T', 1)
        document['count_in_bucket'] = 1
        return splitting_key, [json.dumps(document)]

    def bucket_emails_by_date(self, item1, item2):
        """Merge two buckets of emails of the same time slice into one."""
        document1 = [json.loads(subitem) for subitem in item1]
        document2 = [json.loads(subitem) for subitem in item2]
        new_count = document1[0]['count_in_bucket'] + document2[0]['count_in_bucket']
        merged_documents = document1 + document2
        for i in range(len(merged_documents)):
            merged_documents[i]['count_in_bucket'] = new_count

        return [json.dumps(doc) for doc in merged_documents]

    def convert_from_tuple(self, document_tuple):
        """Convert tuple entry of rdd to usual format for pipeline."""
        return document_tuple[1]

    def dump_items(self, item):
        """Serialize a whole bucket instead of serializing every single item in the bucket itself."""
        documents = [json.loads(subitem) for subitem in item]
        return json.dumps(documents)

    def run(self, rdd):
        """Run topic model bucketing."""
        # if sorting doesn't work because buckets are too big, try sorting before
        # hopefully the use of 'filter()' can be dropped at some point when all docs have a date
        return rdd.filter(lambda item: json.loads(item)['header']['date']) \
                  .map(self.remove_irrelevant_keys) \
                  .map(self.convert_to_tuple) \
                  .reduceByKey(self.bucket_emails_by_date) \
                  .map(self.convert_from_tuple) \
                  .sortBy(lambda item: json.loads(item[0])['date']) \
                  .map(self.dump_items)


class TopicModelPrediction(Pipe):
    """Predict topics for a given text.

    Needs trained lda model + dictionary.
    Will add topic field.
    """

    def __init__(self, conf, read_from='text_clean'):
        """Set params here."""
        super().__init__(conf)
        self.read_from = read_from
        self.conf = conf
        # TODO add variable train_topic_model=Bool to trigger tm training within the main pipeline

    def load_model(self):
        """Load lda model from defined path."""
        with open(os.path.abspath(self.conf.get('topic_modelling', 'file_model')), mode='rb') as pfile:
            model = pickle.load(pfile)

        return model

    def load_dictionary(self):
        """Load dict for lda tm from defined path."""
        with open(os.path.abspath(self.conf.get('topic_modelling', 'file_dictionary')), mode='rb') as pfile:
            dictionary = pickle.load(pfile)

        return dictionary

    def get_topics_for_doc(self, doc_id, text, model, dictionary):
        """Predict topics for a text."""
        def get_word_from_term_id_and_round(word_tuple):
            term = dictionary[word_tuple[0]]
            term_conf = round(float(word_tuple[1]), 8)
            return (term, term_conf)

        bow = dictionary.doc2bow(text.split())
        doc_topics = []

        for topic in model.get_document_topics(bow):
            topic_obj = {}
            topic_id = topic[0]
            term_id_conf_tuples = model.get_topic_terms(topic_id, topn=10)

            topic_obj['topic_id'] = topic_id
            topic_obj['topic_conf'] = round(float(topic[1]), 8)
            topic_obj['terms'] = str(list(map(get_word_from_term_id_and_round, term_id_conf_tuples)))
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
