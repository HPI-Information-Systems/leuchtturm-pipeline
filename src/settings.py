"""This module exports all settings required by pipeline tasks."""


PATH_EMAILS_RAW = 'enron_calo'
PATH_FILES_LISTED = 'tmp/files_listed_dev'
PATH_PIPELINE_RESULTS = 'tmp/pipeline_results_dev'
PATH_LDA_MODEL = 'models/pickled_lda_model.p'
PATH_LDA_DICT = 'models/pickled_lda_dictionary.p'

SOLR_COLLECTION = 'enron_dev'

SOLR_CLIENT_URL = 'http:/0.0.0.0:8983/solr' + '/' + SOLR_COLLECTION
NEO4J_CLIENT_URL = 'bolt://0.0.0.0:7687'

CLUSTER_PARALLELIZATION = 6 * 3 * 3  # num executors * num cores * 3-4
