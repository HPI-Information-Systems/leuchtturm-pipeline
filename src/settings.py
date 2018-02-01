"""This module exports all settings required by pipeline tasks."""

import re


PATH_EMAILS_RAW = 'hdfs://172.18.20.109/LEUCHTTURM/enron_nuix_complete/*/*/*'
PATH_FILES_LISTED = 'hdfs://172.18.20.109/LEUCHTTURM/tmp/files_listed'
PATH_FILES_LISTED_SHORT = re.sub(r'^hdfs://(\d{1,3}\.){3}\d{1,3}', '', PATH_FILES_LISTED)  # don't touch!
PATH_PIPELINE_RESULTS = 'hdfs://172.18.20.109/LEUCHTTURM/tmp/pipeline_results'
PATH_PIPELINE_RESULTS_SHORT = re.sub(r'^hdfs://(\d{1,3}\.){3}\d{1,3}', '', PATH_PIPELINE_RESULTS)  # don't touch!
PATH_LDA_MODEL = '/models/pickled_lda_model.p'
PATH_LDA_DICT = '/models/pickled_lda_dictionary.p'

SOLR_COLLECTION = 'enron_nuix_complete'

HDFS_CLIENT_URL = 'http://172.18.20.109:50070'
SOLR_CLIENT_URL = 'http://b1184.byod.hpi.de:8983/solr' + '/' + SOLR_COLLECTION
NEO4J_CLIENT_URL = 'bolt://b3986.byod.hpi.de:7687'

CLUSTER_PARALLELIZATION = 6 * 3 * 3  # num executors * num cores * 3-4
