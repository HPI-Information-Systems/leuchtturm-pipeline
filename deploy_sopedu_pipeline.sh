#!/bin/bash

set -e # exit script on first failure

if [ $1 == 'master' ]; then
    EMAILS=enron_calo
    PRESULT=tmp/pipeline_results_master
    SOLR=http://sopedu.hpi.uni-potsdam.de:8983/solr/pipeline_master
elif [ $1 == 'dev' ]; then
    EMAILS=enron_calo
    PRESULT=tmp/pipeline_results_dev
    SOLR=http://sopedu.hpi.uni-potsdam.de:8983/solr/pipeline_dev
elif [ $1 == 'mr' ]; then
    EMAILS=enron_calo
    PRESULT=tmp/pipeline_results_mr
    SOLR=http://sopedu.hpi.uni-potsdam.de:8983/solr/pipeline_mr
else
    echo 'No deployment config selected. master, dev or mr possible.'
    exit 1
fi

echo '[stage 1 of 2] Building environment ...'
conda create -n leuchtturm_env python=3.6 -y --copy || true
source activate leuchtturm_env
pip install --quiet -r requirements.txt
# zip py environment
cp -r ~/anaconda2/envs/leuchtturm_env . && cd leuchtturm_env && zip -r --quiet leuchtturm_env.zip * && mv leuchtturm_env.zip .. && cd .. || return
#zip requirements for tm...
cp ~/gitlab-runner/models/* models/ && cd models && zip --quiet models.zip * && mv models.zip .. && cd .. || return
# zip src dir
cd src && zip --quiet src.zip * && mv src.zip .. && cd .. || return
source deactivate

echo '[stage 2 of 2] Running leuchtturm pipeline. This might take a while ...'
hdfs dfs -rm -r $PRESULT || true
curl $SOLR/update\?commit\=true -d  '<delete><query>*:*</query></delete>' || true
PYSPARK_PYTHON=./leuchtturm_env/bin/python \
    spark-submit --master yarn --deploy-mode cluster \
    --driver-memory 8g --executor-memory 4g --num-executors 23 --executor-cores 4 \
    --archives leuchtturm_env.zip#leuchtturm_env,models.zip#models,src.zip#src \
    ./run_pipeline.py --read-from $EMAILS --write-to $PRESULT --solr --solr-url $SOLR 2>/dev/null

echo -e '\n[Done]'
