#!/bin/bash

set -e # exit script on first failure

if [ $1 == 'enron' ]; then
    DATASET=enron
    EMAILS=enron_calo
    PRESULT=tmp/enron_result
    SOLR=http://sopedu.hpi.uni-potsdam.de:8983/solr/enron
elif [ $1 == 'enron-dev' ]; then
    DATASET=enron
    EMAILS=enron_calo
    PRESULT=tmp/enron_dev_result
    SOLR=http://sopedu.hpi.uni-potsdam.de:8983/solr/enron_dev
elif [ $1 == 'dnc' ]; then
    DATASET=dnc
    EMAILS=dnc
    PRESULT=tmp/dnc_result
    SOLR=http://sopedu.hpi.uni-potsdam.de:8983/solr/eval_quagga
else
    echo 'No deployment config selected. enron, enron-dev or dnc possible.'
    exit 1
fi

echo '[stage 1 of 2] Building environment ...'
conda create -n leuchtturm_env python=3.6 -y --copy || true
source activate leuchtturm_env
pip install --quiet -r requirements.txt
# zip py environment
cp -r ~/anaconda2/envs/leuchtturm_env . && cd leuchtturm_env && zip -r --quiet leuchtturm_env.zip * && mv leuchtturm_env.zip .. && cd .. || return
rm -r leuchtturm_env
#zip requirements for tm...
cp ~/gitlab-runner/models/* models/ && cd models && zip --quiet models.zip * && mv models.zip .. && cd .. || return
rm models/*
# zip src dir to ship it as py-files
zip -r --quiet src.zip src || return
# zip config dir to ship it as archives
cd config && zip -r --quiet config.zip * && mv config.zip .. && cd .. || return
source deactivate

echo '[stage 2 of 2] Running leuchtturm pipeline. This might take a while ...'
hdfs dfs -rm -r $PRESULT || true
curl $SOLR/update\?commit\=true -d  '<delete><query>*:*</query></delete>' || true
PYSPARK_PYTHON=./leuchtturm_env/bin/python \
    spark-submit --master yarn --deploy-mode cluster \
    --driver-memory 8g --executor-memory 4g --num-executors 23 --executor-cores 4 \
    --archives leuchtturm_env.zip#leuchtturm_env,models.zip#models,config.zip#config \
    --py-files src.zip \
    run_pipeline.py --read-from $EMAILS --write-to $PRESULT --solr --solr-url $SOLR --dataset $DATASET 2>/dev/null

echo -e '\n[Done]'
