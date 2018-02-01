"""This module exports all settings required by pipeline tasks."""

import re


path_emails_raw = 'hdfs://172.18.20.109/LEUCHTTURM/enron_nuix_testset/*/*/*'
path_files_listed = 'hdfs://172.18.20.109/LEUCHTTURM/tmp/files_nuix_daily'
path_files_listed_short = re.sub(r'^hdfs://(\d{1,3}\.){3}\d{1,3}', '', path_files_listed)  # don't touch!
path_pipeline_results = 'hdfs://172.18.20.109/LEUCHTTURM/tmp/pipeline_nuix_daily'
path_pipeline_results_short = re.sub(r'^hdfs://(\d{1,3}\.){3}\d{1,3}', '', path_pipeline_results)  # don't touch!
path_lda_model = '/models/pickled_lda_model.p'
path_lda_dict = '/models/pickled_lda_dictionary.p'

solr_collection = 'enron_nuix_daily'

hdfs_client_url = 'http://172.18.20.109:50070'
solr_client_url = 'http://b1184.byod.hpi.de:8983/solr' + '/' + solr_collection
neo4j_client_url = 'bolt://b3986.byod.hpi.de:7687'

cluster_parallelization = 6 * 3 * 3  # num executors * num cores * 3-4
