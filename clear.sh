#!/bin/bash
if [ $# -eq 0 ]; then
    echo 'You have to select a config. See config folder!'
    exit 1
fi

source <(python config/ini2bash.py -c $1)

set -x
rm -r $DATA_RESULTS_DIR
rm -r $TOPIC_MODELLING_WORKING_DIR
rm -r $DATA_RESULTS_CORRESPONDENT_DIR
rm -r $DATA_RESULTS_INJECTED_DIR

#curl $SOLR/update\?commit\=true -d  '<delete><query>*:*</query></delete>' || true
#curl $SOLR_TOPICS/update\?commit\=true -d  '<delete><query>*:*</query></delete>' || true
#curl -H "Content-Type: application/json" -X POST \
#     -d '{"statements": [{"statement": "MATCH (n) DETACH DELETE n"}]}' \
#     "http://"$NEO4J_HOST":"$NEO4J_HTTP_PORT"/db/data/transaction/commit" || true
#curl -H "Content-Type: application/json" -X POST \
#     -d '{"statements": [{"statement": "DROP INDEX ON :Person(identifying_name)"}]}' \
#     "http://"$NEO4J_HOST":"$NEO4J_HTTP_PORT"/db/data/transaction/commit" || true
