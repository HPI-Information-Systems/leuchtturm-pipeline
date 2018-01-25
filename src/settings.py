"""This module exports all settings required by pipeline tasks."""


# everything related to hdfs:
_HDFS_NAMENODE = '172.18.20.109'
_HDFS_PORT = '50070'
_LEUCHTTURM_DIR = 'LEUCHTTURM'
_RAW_DOC_DIR = 'enron_nuix_complete/*/*/*'

_FILELISTER_RESULT = 'files_listed_enron'
_PIPELINE_RESULT = 'pipeline_results_enron'

# everything related to solr_collection
_SOLR_CLIENT = 'http://b1184.byod.hpi.de:8983/solr'
_SOLR_COLLECTION = 'enron_nuix'

# everything neo4j related
_NEO4J_URI = 'bolt://b3986.byod.hpi.de:7687'

# everythin cluster related
_NUM_WORKERS = 6
_NUM_CORES_PER_WORKER = 3


# exports:
raw_data_path_spark = 'hdfs://' + _HDFS_NAMENODE + '/' + _LEUCHTTURM_DIR + '/' + _RAW_DOC_DIR
file_lister_path_spark = 'hdfs://' + _HDFS_NAMENODE + '/' + _LEUCHTTURM_DIR + '/' + _FILELISTER_RESULT
pipeline_result_path_spark = 'hdfs://' + _HDFS_NAMENODE + '/' + _LEUCHTTURM_DIR + '/' + _PIPELINE_RESULT
hdfs_client_url = 'http://' + _HDFS_NAMENODE + ':' + _HDFS_PORT
pipeline_result_path_hdfs_client = '/' + _LEUCHTTURM_DIR + '/' + _PIPELINE_RESULT
solr_url = _SOLR_CLIENT + '/' + _SOLR_COLLECTION
solr_collection = _SOLR_COLLECTION
neo4j_uri = _NEO4J_URI
cluster_parallelization = _NUM_WORKERS * _NUM_CORES_PER_WORKER * 3
