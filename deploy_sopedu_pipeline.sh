#!/bin/bash

set -e # exit script on first failure

if [ $# -eq 0 ]; then
    echo 'You have to select a config. See config folder!'
    exit 1
fi

echo '[stage 1 of 2] Building environment ...'
conda create -n leuchtturm_env python=3.6 -y --copy || true
source activate leuchtturm_env
pip install --quiet -r requirements.txt
conda install -c conda-forge nltk_data -y
# zip py environment
cp -r ~/anaconda2/envs/leuchtturm_env . && cd leuchtturm_env && zip -r --quiet leuchtturm_env.zip * && mv leuchtturm_env.zip .. && cd .. || return
rm -r leuchtturm_env
#zip requirements for tm...
cp ~/gitlab-runner/models/* models/ && cd models && zip --quiet models.zip * && mv models.zip .. && cd .. || return
rm models/*
# zip src dir to ship it as py-files
zip -r --quiet src.zip src || return
# zip snap dir to ship it as archives
cd snap && zip -r --quiet snap_raw.zip * && mv snap_raw.zip .. && cd .. || return
zip -r --quiet snap.zip snap || return
# zip config dir to ship it as archives
cd config && zip -r --quiet config_raw.zip * && mv config_raw.zip .. && cd .. || return
zip -r --quiet config.zip config || return


# load variables from selected config
source <(python config/ini2bash.py -c $1)
SOLR=$SOLR_URL$SOLR_COLLECTION
SOLR_TOPICS=$SOLR_URL$SOLR_TOPIC_COLLECTION

source deactivate

echo '[stage 2 of 2] Running leuchtturm pipeline. This might take a while ...'

hdfs dfs -rm -r $DATA_RESULTS_DIR || true
hdfs dfs -rm -r $DATA_RESULTS_TOPICS_DIR || true
hdfs dfs -rm -r $DATA_RESULTS_CORRESPONDENT_DIR || true
hdfs dfs -rm -r $DATA_RESULTS_INJECTED_DIR || true

curl $SOLR/update\?commit\=true -d  '<delete><query>*:*</query></delete>' || true
curl $SOLR_TOPICS/update\?commit\=true -d  '<delete><query>*:*</query></delete>' || true

curl -H "Content-Type: application/json" -X POST \
     -d '{"statements": [{"statement": "MATCH (n) DETACH DELETE n"}]}' \
     "http://"$NEO4J_HOST":"$NEO4J_HTTP_PORT"/db/data/transaction/commit" || true
curl -H "Content-Type: application/json" -X POST \
     -d '{"statements": [{"statement": "DROP INDEX ON :Person(identifying_name)"}]}' \
     "http://"$NEO4J_HOST":"$NEO4J_HTTP_PORT"/db/data/transaction/commit" || true

PYSPARK_PYTHON=./leuchtturm_env/bin/python \
    spark-submit --master yarn --deploy-mode cluster \
    --driver-memory $SPARK_DRIVER_MEMORY --executor-memory $SPARK_EXECUTOR_MEMORY \
    --num-executors $SPARK_NUM_EXECUTORS --executor-cores $SPARK_EXECUTOR_CORES \
    --archives leuchtturm_env.zip#leuchtturm_env,models.zip#models,config_raw.zip#config,snap_raw.zip#snap \
    --py-files src.zip,config.zip,snap.zip \
    run_pipeline.py -c=$1 2>/dev/null

echo -e '\n[Done]'
