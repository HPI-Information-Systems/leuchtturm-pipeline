#!/bin/bash

EMAILS=emails
FLISTER=temp/files_listed
PRESULT=temp/pipeline_results

set -e # exit script on first failure

# export SPARK_HOME=/usr/hdp/2.6.2.0-205/spark2/
echo '[stage 1 of 2] Running file lister ...'
python src/file_lister.py $EMAILS $FLISTER
ls $FLISTER/_SUCCESS
echo '[stage 2 of 2] Running leuchtturm pipeline ...'
python src/run_leuchtturm.py $FLISTER $PRESULT
ls $PRESULT/_SUCCESS

echo -e '\n[Done]\n\Head of pipeline results:\n'
head -n 1 $PRESULT/part-00000 | python -m json.tool --sort-keys
cat $PRESULT/part-00000 > pipeline_results.txt
