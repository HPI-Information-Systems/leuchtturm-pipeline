#!/bin/bash

set -e # exit script on first failure

if [ $1 == 'enron' ]; then
    EMAILS=enron_calo
    PRESULT=tmp/enron_result
    SOLR=http://sopedu.hpi.uni-potsdam.de:8983/solr/enron
elif [ $1 == 'enron-dev' ]; then
    EMAILS=enron_calo
    PRESULT=tmp/enron_dev_result
    SOLR=http://sopedu.hpi.uni-potsdam.de:8983/solr/enron_dev
elif [ $1 == 'dnc' ]; then
    EMAILS=dnc
    PRESULT=tmp/dnc_result
    SOLR=http://sopedu.hpi.uni-potsdam.de:8983/solr/dnc
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
#zip requirements for tm...
cp ~/gitlab-runner/models/* models/ && cd models && zip --quiet models.zip * && mv models.zip .. && cd .. || return
# zip src dir to ship it as py-files
zip -r --quiet src.zip src || return
# zip config dir to ship it as py-files
zip -r --quiet config.zip config || return
source deactivate

echo '[stage 2 of 2] Running leuchtturm pipeline. This might take a while ...'
hdfs dfs -rm -r $PRESULT || true
curl $SOLR/update\?commit\=true -d  '<delete><query>*:*</query></delete>' || true
PYSPARK_PYTHON=./leuchtturm_env/bin/python \
    spark-submit --master yarn --deploy-mode cluster \
    --driver-memory 8g --executor-memory 4g --num-executors 23 --executor-cores 4 \
    --archives leuchtturm_env.zip#leuchtturm_env,models.zip#models \
    --py-files src.zip config.zip \
    run_pipeline.py --read-from $EMAILS --write-to $PRESULT --solr --solr-url $SOLR 2>/dev/null

echo -e '\n[Done]'
