"""Make show_topics() accessible."""
import pickle
import os
from config.config import Config


def show_topics(conf):
    """Show all topics of saved saved LDA model."""
    with open(os.path.abspath(conf.get('topic_modelling', 'file_model')), mode='rb') as pfile:
        model = pickle.load(pfile)
    with open(os.path.abspath(conf.get('topic_modelling', 'file_dictionary')), mode='rb') as pfile:
        dictionary = pickle.load(pfile)

    def get_word_from_term_id_and_round(word_tuple):
        term = dictionary[word_tuple[0]]
        term_conf = round(float(word_tuple[1]), 8)
        return (term, term_conf)

    for i in range(0, 100):
        term_id_conf_tuples = model.get_topic_terms(i, topn=20)
        print('topic #', i, list(map(get_word_from_term_id_and_round, term_id_conf_tuples)))


show_topics(Config())
