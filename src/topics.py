"""Topic modeling pipes for leuchtturm pipelines."""

from collections import defaultdict
import ujson as json
import pickle
from string import punctuation
import numpy as np
from scipy.linalg import norm
from gensim.models.ldamodel import LdaModel
from gensim.corpora import Dictionary
from nltk import word_tokenize
from nltk.corpus import stopwords as nltksw
from nltk.stem.wordnet import WordNetLemmatizer
from textacy.preprocess import preprocess_text as textacy_preprocess
from datetime import datetime
from string import whitespace
import re

from .common import Pipe


class TopicModelTrainingOld(Pipe):
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

        dictionary = Dictionary(processed_corpus)
        with open(self.conf.get('topic_modelling', 'file_dictionary'), 'wb') as pfile:
            pickle.dump(dictionary, pfile)

        bow_corpus = [dictionary.doc2bow(text) for text in processed_corpus]

        lda = LdaModel(bow_corpus, num_topics=num_topics, iterations=iterations, eta=eta, alpha=alpha)
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
        self.stopwords = set(nltksw.words('english'))
        self.lemma = WordNetLemmatizer()

    def _normalize_split_correspondent_names(self, sender, recipients):
        sender_parts = {part.lower() for part in sender['name'].split()}
        recipient_parts = set()
        for recipient in recipients:
            for part in recipient['name'].split():
                recipient_parts.add(part.lower())
        return sender_parts, recipient_parts

    def remove_salutation(self, body):
        """Remove salutation operators from the body if applicable."""
        salutation_operators = \
            {'hi', 'hello', 'hey', 'all', 'dear', 'sir', 'madam', 'good', 'morning', 'afternoon', 'greetings'}

        def remove_salutation_operators(bow):
            return [word for word in bow if word not in salutation_operators]

        split_body = body.split('\n', 1)
        if len(split_body) != 2:
            return body
        salutation_candidate = word_tokenize(split_body[0])
        body_candidate = split_body[1]

        return ' '.join(remove_salutation_operators(salutation_candidate)) + '\n' + body_candidate

    def tokenize_and_remove_various_words(self, body, name_parts):
        """Clean the bow from distracting words."""
        body = textacy_preprocess(
            body,
            no_urls=True,
            no_emails=True,
            no_phone_numbers=True,
            no_numbers=True,
            no_currency_symbols=True,
            no_contractions=True,
            no_accents=True
        )
        textacy_placeholders = ['*URL*', '*EMAIL*', '*PHONE*', '*NUMBER*']

        bow = word_tokenize(body)
        processed_bow = []

        for word in bow:
            # remove placeholder words from textacy
            for placeholder_text in textacy_placeholders:
                word = word.replace(placeholder_text, '')
            # remove names of the sender and the recipients of the email
            word = word if word not in name_parts else ''
            # remove punctuation characters
            word = re.sub(r'[' + re.escape(punctuation) + r']', '', word)
            # remove stopwords
            word = word if word not in self.stopwords else ''
            # remove very short words as they often don't carry any meaning
            word = word if len(word) > 2 else ''
            if word:
                processed_bow.append(word)

        return processed_bow

    def lemmatize(self, bow):
        """Lemmatize each word in a bow as well as possible."""
        processed_bow = []

        for word in bow:
            lemmatized_word_noun = self.lemma.lemmatize(word, 'n')
            lemmatized_word_verb = self.lemma.lemmatize(word, 'v')
            if word != lemmatized_word_noun and word != lemmatized_word_verb:
                processed_bow.append(min([lemmatized_word_noun, lemmatized_word_verb], key=len))
                continue
            elif word != lemmatized_word_noun:
                processed_bow.append(lemmatized_word_noun)
                continue
            elif word != lemmatized_word_verb:
                processed_bow.append(lemmatized_word_verb)
                continue

            lemmatized_word_adjective = self.lemma.lemmatize(word, 'a')
            if word != lemmatized_word_adjective:
                processed_bow.append(lemmatized_word_adjective)
                continue

            processed_bow.append(word)

        return processed_bow

    def run_on_document(self, item):
        """Run TM preprocessing on document."""
        document = json.loads(item)

        sender_name_parts, recipient_name_parts = self._normalize_split_correspondent_names(
            document['header']['sender'], document['header']['recipients']
        )
        body = document[self.read_from].strip(whitespace).lower()
        body = self.remove_salutation(body)
        bow = self.tokenize_and_remove_various_words(body, sender_name_parts.union(recipient_name_parts))
        bow = self.lemmatize(bow)

        document[self.write_to] = bow
        return json.dumps(document)

    def run(self, rdd):
        """Run pipe in spark context."""
        print('lt_logs', datetime.now(), 'Starting TM preprocessing...')
        rdd = rdd.map(self.run_on_document)
        return rdd


class TopicModelTraining(Pipe):
    """Train topic model and return it.

    Create a dictionary from the corpus that can be used with the lda topic model.
    Train a lda topic model using gensim.
    Return the topic model to be used by downstream tasks.
    """

    def __init__(self, conf, read_from='bow'):
        """Set params."""
        super().__init__(conf)
        self.conf = conf
        self.read_from = read_from

    def create_dictionary(self, corpus):
        """Create a gensim Dictionary that can be used to convert documents so that they can be used by LDAModel."""
        print('lt_logs', datetime.now(), 'Starting dictionary creation...')

        dictionary = Dictionary(corpus)

        dict_words = [word for word in dictionary.values()]
        dictionary.filter_extremes(
            no_above=self.conf.get('tm_preprocessing', 'maximum_fraction_word_document_frequency'),
            no_below=0,
            keep_n=len(dict_words)
        )
        dict_words_wo_frequent = [word for word in dictionary.values()]
        dictionary.filter_extremes(
            no_above=1.0,
            no_below=self.conf.get('tm_preprocessing', 'minimum_total_word_document_frequency'),
            keep_n=len(dict_words)
        )
        dict_words_wo_infrequent = [word for word in dictionary.values()]

        removed_frequent_words = set(dict_words) - set(dict_words_wo_frequent)
        removed_infrequent_words = set(dict_words_wo_frequent) - set(dict_words_wo_infrequent)
        try:
            with open(self.conf.get('tm_preprocessing', 'file_removed_frequent_words'), 'wb') as file:
                file.write(str(removed_frequent_words).encode())
            with open(self.conf.get('tm_preprocessing', 'file_removed_infrequent_words'), 'wb') as file:
                file.write(str(removed_infrequent_words).encode())
            with open(self.conf.get('topic_modelling', 'file_dictionary'), 'wb') as pfile:
                pickle.dump(dictionary, pfile)
        except Exception:
            print('lt_logs', datetime.now(), 'Saving the TM dictionary and frequency-removed words to disk didnt work')

        print('lt_logs', datetime.now(), 'Finished dictionary creation.')
        return dictionary

    def run(self, rdd):
        """Run topic model training."""
        corpus = rdd.map(lambda x: json.loads(x)[self.read_from]).collect()
        dictionary = self.create_dictionary(corpus)
        corpus_dictionarized = [dictionary.doc2bow(document) for document in corpus]

        print('lt_logs', datetime.now(), 'Starting TM training...')

        lda = LdaModel(
            corpus_dictionarized,
            num_topics=self.conf.get('topic_modelling', 'num_topics'),
            iterations=self.conf.get('topic_modelling', 'iterations'),
            eta=self.conf.get('topic_modelling', 'eta'),
            alpha=self.conf.get('topic_modelling', 'alpha_numerator') / self.conf.get('topic_modelling', 'num_topics')
        )
        try:
            with open(self.conf.get('topic_modelling', 'file_model'), 'wb') as pfile:
                pickle.dump(lda, pfile)
        except Exception:
            print('lt_logs', datetime.now(), 'Saving the TM to disk didnt work')

        print('lt_logs', datetime.now(), 'Finished TM training.')
        return lda, dictionary


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
        for doc in merged_documents:
            doc['count_in_bucket'] = new_count

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

    def __init__(self, conf, topic_ranks, read_from, model, dictionary):
        """Set params here."""
        super().__init__(conf)
        self.conf = conf
        self.topic_ranks = topic_ranks
        self.read_from = read_from
        self.model = model
        self.dictionary = dictionary

    def get_word_from_word_id_and_round(self, word_tuple):
        """Map a tuple of word_id and conf to a tuple of actual word and rounded conf."""
        word = self.dictionary.value[word_tuple[0]].replace("'", '')  # TODO: integrate stemmer that takes care of this
        word_conf = round(float(word_tuple[1]), 8)
        return (word, word_conf)

    def get_topics_for_doc(self, doc_id, bow):
        """Predict topics for a text."""
        bow_dictionarized = self.dictionary.value.doc2bow(bow)

        doc_topics = []
        for topic in self.model.value.get_document_topics(
                bow_dictionarized,
                minimum_probability=self.conf.get('topic_modelling', 'minimum_prediction_probability')
        ):
            topic_obj = {}
            topic_id = topic[0]
            word_id_conf_tuples = self.model.value.get_topic_terms(topic_id, topn=10)

            topic_obj['topic_id'] = topic_id
            topic_obj['topic_rank'] = self.topic_ranks[topic_id][1]
            topic_obj['topic_conf'] = round(float(topic[1]), 8)
            topic_obj['terms'] = str(list(map(self.get_word_from_word_id_and_round, word_id_conf_tuples)))
            topic_obj['doc_id'] = doc_id

            doc_topics.append(topic_obj)

        return doc_topics

    def run_on_document(self, raw_message):
        """Predict topics for a leuchtturm document."""
        document = json.loads(raw_message)
        doc_topics = self.get_topics_for_doc(document['doc_id'], document[self.read_from])

        return [json.dumps(topic, ensure_ascii=False) for topic in doc_topics]

    def run_on_partition(self, partition):
        """Run task in spark context. Partitionwise for performance reasosn."""
        for doc in partition:
            yield self.run_on_document(doc)

    def run(self, rdd):
        """Run task in spark context."""
        return rdd.mapPartitions(lambda x: self.run_on_partition(x)) \
                  .flatMap(lambda x: x)


class TopicSimilarity(Pipe):
    """Calculate topic similarity."""

    def __init__(self, conf, model):
        """Set params here."""
        super().__init__(conf)
        self.conf = conf
        self.model = model.value

    def run(self):
        """Run topic similarity calc."""
        model_topics = self.model.get_topics()

        def hellinger(p, q):
            return norm(np.sqrt(p) - np.sqrt(q)) / np.sqrt(2)

        # 1 topic, 10 words, first item in list, id
        most_significant_topic_id = self.model.print_topics(1, 10)[0][0]

        def get_smallest_distance_to_reference(current_topic, remaining_topics):

            # init with invalid neg topic id and max possible ditstance
            smallest_dist = (-1, 1)

            for topic_id in remaining_topics:
                dist_to_ref = hellinger(model_topics[current_topic], model_topics[topic_id])
                smallest_dist = (topic_id, dist_to_ref) if dist_to_ref <= smallest_dist[1] else smallest_dist

            return smallest_dist

        topics_by_rank = []
        remaining_topics = list(range(len(model_topics)))
        count = 0
        current_topic = most_significant_topic_id

        while remaining_topics:
            remaining_topics.remove(current_topic)

            smallest_distance_to_current = get_smallest_distance_to_reference(current_topic, remaining_topics)

            topics_by_rank.append({
                'rank': count,
                'id': current_topic,
                'distance_to_next': smallest_distance_to_current[1]
            })

            current_topic = smallest_distance_to_current[0]
            count += 1

        topics_by_id = sorted(topics_by_rank, key=lambda x: x['id'], reverse=False)

        rank_array = list(map(
            lambda x: (x['id'], x["rank"]), topics_by_id
        ))

        return rank_array
