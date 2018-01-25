"""This module exports all settings required by pipeline tasks."""


# build name
_BUILD_NAME = 'enron_nuix'

# everything related to hdfs:
_HDFS_NAMENODE = '172.18.20.109'
_HDFS_PORT = '50070'
_LEUCHTTURM_DIR = 'LEUCHTTURM'
_RAW_DOC_DIR = 'enron_nuix_complete/*/*/*'

_FILELISTER_RESULT = '_files_listed'
_PIPELINE_RESULT = '_pipeline_results'

# everything related to solr_collection
_SOLR_CLIENT = 'http://b1184.byod.hpi.de:8983/solr'

# everything neo4j related
_NEO4J_URI = 'bolt://b3986.byod.hpi.de:7687'

# everythin cluster related
_NUM_WORKERS = 6
_NUM_CORES_PER_WORKER = 3


# exports:
build_name = _BUILD_NAME
raw_data_path_spark = 'hdfs://' + _HDFS_NAMENODE + '/' + _LEUCHTTURM_DIR + '/' + _RAW_DOC_DIR
file_lister_path_spark = 'hdfs://' + _HDFS_NAMENODE + '/' + _LEUCHTTURM_DIR + '/' + _BUILD_NAME + _FILELISTER_RESULT
pipeline_result_path_spark = 'hdfs://' + _HDFS_NAMENODE + '/' + _LEUCHTTURM_DIR + '/' + _BUILD_NAME + _PIPELINE_RESULT
hdfs_client_url = 'http://' + _HDFS_NAMENODE + ':' + _HDFS_PORT
pipeline_result_path_hdfs_client = '/' + _LEUCHTTURM_DIR + '/' + _BUILD_NAME + _PIPELINE_RESULT
file_lister_path_hdfs_client = '/' + _LEUCHTTURM_DIR + '/' + _BUILD_NAME + _FILELISTER_RESULT
solr_url = _SOLR_CLIENT + '/' + _BUILD_NAME
solr_collection = _BUILD_NAME
neo4j_uri = _NEO4J_URI
cluster_parallelization = _NUM_WORKERS * _NUM_CORES_PER_WORKER * 3
