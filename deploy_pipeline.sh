#!/bin/bash

echo 'Building conda environment for pipeline ...'
conda create -n leuchtturm_env python=3.6 -y --copy || true
source activate leuchtturm_env
pip install -r requirements.txt
cd src
cp -r ~/anaconda2/envs/leuchtturm_env .
zip -r leuchtturm_env.zip leuchtturm_env
source deactivate
echo 'Deleting files_listed_dev on hdfs ...'
hdfs dfs -rm -r tmp/files_listed_dev || true
echo 'Running file lister'
export SPARK_HOME=/usr/hdp/2.6.2.0-205/spark2/
PYSPARK_PYTHON=./LEUCHTTURM_ENV/leuchtturm_env/bin/python spark-submit --master yarn --deploy-mode cluster --driver-memory 4g --executor-memory 4g --num-executors 6 --executor-cores 3 --archives leuchtturm_env.zip#LEUCHTTURM_ENV --py-files settings.py file_lister.py
echo 'Deleting pipeline_results_dev on hdfs ...'
hdfs dfs -rm -r tmp/pipeline_results_dev || true
echo 'Running leuchtturm pipeline...'
PYSPARK_PYTHON=./LEUCHTTURM_ENV/leuchtturm_env/bin/python spark-submit --master yarn --deploy-mode cluster --driver-memory 4g --executor-memory 4g --num-executors 6 --executor-cores 3 --archives leuchtturm_env.zip#LEUCHTTURM_ENV --py-files settings.py,leuchtturm.py run_leuchtturm.py
echo 'Running db uploads...'
# source activate leuchtturm_env
# python write_to_solr.py &
# python write_to_neo4j
