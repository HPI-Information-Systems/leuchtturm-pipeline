#!/bin/bash

EMAILS=enron_calo
FLISTER=temp/files_listed
PRESULT=temp/pipeline_results

set -e # exit script on first failure

echo '[stage 1 of 4] Building conda environment ...'
conda create -n leuchtturm_env python=3.6 -y --copy || true
source activate leuchtturm_env
pip install --quiet -r requirements.txt
cp -r ~/anaconda2/envs/leuchtturm_env . && cd leuchtturm_env && zip -r --quiet leuchtturm_env.zip * && cd ..
cd models && zip --quiet models.zip * && cd ..
source deactivate

echo '[stage 2 of 4] Running file lister ...'
PYSPARK_PYTHON=./leuchtturm_env/bin/python \
    spark-submit --master yarn --deploy-mode cluster \
    --driver-memory 20g --executor-memory 20g --num-executors 8 --executor-cores 10 \
    --archives leuchtturm_env.zip#leuchtturm_env \
    --py-files src/settings.py \
    src/file_lister.py $EMAILS $FLISTER
echo '[stage 3 of 4] Running leuchtturm pipeline ...'
PYSPARK_PYTHON=./leuchtturm_env/bin/python \
    spark-submit --master yarn --deploy-mode cluster \
    --driver-memory 20g --executor-memory 20g --num-executors 8 --executor-cores 10 \
    --archives leuchtturm_env.zip#leuchtturm_env,models.zip#models \
    --py-files src/settings.py,src/leuchtturm.py \
    src/run_leuchtturm.py $FLISTER $PRESULT

echo '[stage 4 of 4] Running db uploads ...'
# source activate leuchtturm_env
# python write_to_solr.py
# python write_to_neo4j

echo -e '\n[Done]\n\Head of pipeline results:\n'
hdfs dfs -cat tmp/pipeline_results_dev | head -n 1 | python -m json.tool --sort-keys
