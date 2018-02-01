"""This module exports all settings required by pipeline tasks."""

import re


path_emails_raw = 'hdfs://172.18.20.109/LEUCHTTURM/enron_calo_flat/*/*'
path_files_listed = 'hdfs://172.18.20.109/LEUCHTTURM/tmp/files_listed_calo_daily'
path_files_listed_short = re.sub(r'^hdfs://(\d{1,3}\.){3}\d{1,3}', '', path_files_listed)  # don't touch!
path_pipeline_results = 'hdfs://172.18.20.109/LEUCHTTURM/tmp/pipeline_results_calo_daily'
path_pipeline_results_short = re.sub(r'^hdfs://(\d{1,3}\.){3}\d{1,3}', '', path_pipeline_results)  # don't touch!

solr_collection = 'enron_calo_daily'

hdfs_client_url = 'http://172.18.20.109:50070'
solr_client_url = 'http://b1184.byod.hpi.de:8983/solr' + '/' + solr_collection
neo4j_client_url = 'bolt://b3986.byod.hpi.de:7687'

cluster_parallelization = 6 * 3 * 3  # num executors * num cores * 3-4
