#!/bin/bash

EMAILS=emails
PRESULT=tmp/pipeline_results

set -e # exit script on first failure

echo '[stage 1 of 3] Building environment ...'
pip install --quiet -r requirements.txt

echo '[stage 2 of 3] Fetching data ...'
# get requirements that are not stored in git
cp ~/gitlab-runner/models/* models/
cp ~/gitlab-runner/emails/* $EMAILS/

# export SPARK_HOME=/usr/hdp/2.6.2.0-205/spark2/
echo '[stage 3 of 3] Running pipeline ...'
python src/run_pipeline.py --read_from $EMAILS --write_to $PRESULT
ls $PRESULT/_SUCCESS

echo -e '\n[Done]\n\nHead of pipeline results:\n'
cat $PRESULT/* | head -n 1 | python -m json.tool
cat $PRESULT/* > result.txt
