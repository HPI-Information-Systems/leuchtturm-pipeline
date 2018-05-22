#!/bin/bash

set -e # exit script on first failure

# load variables from selected config
source <(python config/ini2bash.py -c config/testconfig.ini)


echo '[stage 1 of 3] Building environment ...'
pip install --quiet -r requirements.txt

echo '[stage 2 of 3] Fetching data ...'
# get requirements that are not stored in git
cp ~/gitlab-runner/models/* $MODELS_DIRECTORY
cp ~/gitlab-runner/emails/* $DATA_SOURCE_DIR

# export SPARK_HOME=/usr/hdp/2.6.2.0-205/spark2/
echo '[stage 3 of 3] Running pipeline ...'
python ./run_pipeline.py -c config/testconfig.ini
ls $DATA_RESULTS_DIR/_SUCCESS

echo -e '\n[Done]\n\nHead of pipeline results:\n'
cat $DATA_RESULTS_DIR/* | head -n 1 | python -m json.tool
cat $DATA_RESULTS_DIR/* > result.txt
