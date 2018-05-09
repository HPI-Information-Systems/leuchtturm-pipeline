"""Topic modeling pipes for leuchtturm pipelines."""

from collections import defaultdict
import ujson as json
import pickle
from string import punctuation

from gensim.models import ldaseqmodel
from gensim.corpora import Dictionary
from nltk.corpus import stopwords as nltksw
from nltk.stem.wordnet import WordNetLemmatizer

from .common import Pipe


class TopicModelPreprocessing(Pipe):
    def __init__(self, read_from, write_to, min_freq=0, max_percentage=1.00):
        super().__init__()
        self.read_from = read_from
        self.write_to = write_to
        self.stopwords = nltksw.words('english')
        self.lemma = WordNetLemmatizer()
        self.word_frequencies = defaultdict(lambda: 0)
        self.high_freq_words = set()
        self.low_freq_words = set()
        self.max_percentage = max_percentage
        self.max_freq = -1
        self.min_freq = min_freq


    def tokenize(self, body):
        return body.lower().split()


    def remove_various_words(self, bow, sender, recipients):
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
        return [self.lemma.lemmatize(word) for word in bow]

    def count_word_frequencies(self, bow):
        for word in bow:
            self.word_frequencies[word] += 1

    def run_on_document(self, item):
        """Transform a document into clean text."""
        document = json.loads(item)
        bow = self.tokenize(document[self.read_from])

        bow = self.remove_various_words(bow, document['header']['sender'], document['header']['recipients'])
        bow = self.lemmatize(bow)
        self.count_word_frequencies(bow)

        document[self.write_to] = bow
        return json.dumps(document)

    def filter_by_frequencies(self, item):
        document = json.loads(item)
        bow = document[self.write_to]

        filtered_bow = []
        for word in bow:
            if self.word_frequencies[word] > self.min_freq \
               and self.word_frequencies[word] < self.max_freq:
                filtered_bow.append(word)

        document[self.write_to] = filtered_bow
        return json.dumps(document)

    # def filter_keys(self, item):
    #     document = json.loads(item)
    #     for key in list(set(document.keys()) - {'body', 'bow'}):
    #         document.pop(key)
    #     return json.dumps(document)

    def run(self, rdd):
        """Run pipe in spark context."""
        self.max_freq = self.max_percentage * rdd.count()
        rdd = rdd.map(self.run_on_document)
        return rdd.map(self.filter_by_frequencies)
                  # .map(self.filter_dont_push)


class TopicModelBucketing(Pipe):
    """Train topic model and export it.

    Train a lda topic model using gensim.
    Export pickeled model to a textfile.
    """

    def __init__(self, read_from='bow'):
        super().__init__()
        self.read_from = read_from

    def remove_irrelevant_keys(self, item):
        document = json.loads(item)
        date = document['header']['date']
        for key in list(set(document.keys()) - {self.read_from}):
            document.pop(key)
        document['date'] = date
        return json.dumps(document)

    def convert_to_tuple(self, item, bucket_timeframe='month'):
        document = json.loads(item)
        splitting_key = document['date']

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
        document1 = [json.loads(subitem) for subitem in item1]
        document2 = [json.loads(subitem) for subitem in item2]
        new_count = document1[0]['count_in_bucket'] + document2[0]['count_in_bucket']
        merged_documents = document1 + document2
        for i in range(len(merged_documents)):
            merged_documents[i]['count_in_bucket'] = new_count

        return [json.dumps(doc) for doc in merged_documents]

    def convert_from_tuple(self, document_tuple):
        """Convert tupel entry of rdd to usual format for pipeline."""
        return document_tuple[1]

    def dump_items(self, item):
        documents = [json.loads(subitem) for subitem in item]
        return json.dumps(documents)

    def run(self, rdd):
        """Run topic model training."""
        # if sorting doesn't work because buckets are too big, try sorting before
        # hopefully the use of 'filter()' can be dropped at some point when all docs have a date
        return rdd.filter(lambda item: json.loads(item)['header']['date']) \
           .map(self.remove_irrelevant_keys) \
           .map(self.convert_to_tuple) \
           .reduceByKey(self.bucket_emails_by_date) \
           .map(self.convert_from_tuple) \
           .sortBy(lambda item: json.loads(item[0])['date']) \
           .map(self.dump_items)


class TopicModelTraining(Pipe):
    def __init__(self, read_from='bow', num_topics=100, eta=0.1):
        """TODO: set params here (iterations, num_topics, ...)!! Especially output paths."""
        super().__init__()
        self.read_from = read_from
        self.num_topics = num_topics
        self.eta = eta
        self.alpha = 50 / num_topics

    def run(self, rdd):
        buckets = rdd.collect()
        bows = [doc['bow'] for bucket in buckets for doc in json.loads(bucket)]

        dictionary = Dictionary(bows)
        bow_corpus = [dictionary.doc2bow(bow) for bow in bows]
        time_slices = [int(json.loads(bucket)[0]['count_in_bucket']) for bucket in buckets]

        ldaseq = ldaseqmodel.LdaSeqModel(corpus=bow_corpus, id2word=dictionary, time_slice=time_slices, num_topics=10)

        with open('./models/pickled_ldaseq_dictionary.p', 'wb') as pfile:
            pickle.dump(dictionary, pfile)
        with open('./models/pickled_ldaseq_model.p', 'wb') as pfile:
            pickle.dump(ldaseq, pfile)


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
