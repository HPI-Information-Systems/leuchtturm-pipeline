#!/bin/bash

set -e # exit script on first failure

if [ $1 == 'master' ]; then
    EMAILS=enron_calo
    FLISTER=temp/files_listed_master
    PRESULT=temp/pipeline_results_master
    PRESULT_=hdfs://odin01.hpi.uni-potsdam.de:8020/user/leuchtturm/temp/pipeline_results_master
    SOLR=http://sopedu.hpi.uni-potsdam.de:8983/solr/pipeline_master
elif [ $1 == 'dev' ]; then
    EMAILS=enron_calo
    FLISTER=temp/files_listed_dev
    PRESULT=temp/pipeline_results_dev
    PRESULT_=hdfs://odin01.hpi.uni-potsdam.de:8020/user/leuchtturm/temp/pipeline_results_dev
    SOLR=http://sopedu.hpi.uni-potsdam.de:8983/solr/pipeline_dev
elif [ $1 == 'mr' ]; then
    EMAILS=enron_calo
    FLISTER=temp/files_listed_mr
    PRESULT=temp/pipeline_results_mr
    PRESULT_=hdfs://odin01.hpi.uni-potsdam.de:8020/user/leuchtturm/temp/pipeline_results_mr
    SOLR=http://sopedu.hpi.uni-potsdam.de:8983/solr/pipeline_mr
else
    echo 'No deployment config selected. master, dev or mr possible.'
    exit 1
fi

echo '[stage 1 of 4] Building conda environment ...'
# conda create -n leuchtturm_env python=3.6 -y --copy || true
# source activate leuchtturm_env
# pip install --quiet -r requirements.txt
# cp -r ~/anaconda2/envs/leuchtturm_env . && cd leuchtturm_env && zip -r --quiet leuchtturm_env.zip * && mv leuchtturm_env.zip .. && cd .. || return
# cp ~/gitlab-runner/models/* models/ && cd models && zip --quiet models.zip * && mv models.zip .. && cd .. || return
# source deactivate

# echo '[stage 2 of 4] Running file lister ...'
# hdfs dfs -rm -r $FLISTER || true
# PYSPARK_PYTHON=./leuchtturm_env/bin/python \
#     spark-submit --master yarn --deploy-mode cluster \
#     --driver-memory 8g --executor-memory 4g --num-executors 23 --executor-cores 4 \
#     --archives leuchtturm_env.zip#leuchtturm_env \
#     --py-files src/settings.py \
#     src/file_lister.py $EMAILS $FLISTER
# echo '[stage 3 of 4] Running leuchtturm pipeline ...'
# hdfs dfs -rm -r $PRESULT || true
# PYSPARK_PYTHON=./leuchtturm_env/bin/python \
#     spark-submit --master yarn --deploy-mode cluster \
#     --driver-memory 8g --executor-memory 4g --num-executors 23 --executor-cores 4 \
#     --archives leuchtturm_env.zip#leuchtturm_env,models.zip#models \
#     --py-files src/settings.py,src/leuchtturm.py \
#     src/run_leuchtturm.py $FLISTER $PRESULT

echo '[stage 4 of 4] Running db uploads ...'
source activate leuchtturm_env
curl $SOLR/update\?commit\=true -d  '<delete><query>*:*</query></delete>' || true
PYSPARK_PYTHON=./leuchtturm_env/bin/python \
    spark-submit --master yarn --deploy-mode cluster \
    --driver-memory 8g --executor-memory 8g --num-executors 23 --executor-cores 4 \
    --archives leuchtturm_env.zip#leuchtturm_env \
    --py-files src/settings.py \
    src/write_to_solr.py $PRESULT_ $SOLR
# python write_to_neo4j

echo -e '\n[Done]\n\Head of pipeline results:\n'
hdfs dfs -cat $PRESULT/* | head -n 1 | python -m json.tool
hdfs dfs -cat $PRESULT/* | head -n 20 > result.txt
