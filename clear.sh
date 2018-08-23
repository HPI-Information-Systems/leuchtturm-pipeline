#!/bin/bash
if [ $# -eq 0 ]; then
    echo 'You have to select a config. See config folder!'
    exit 1
fi

source <(python config/ini2bash.py -c $1)

set -x
rm -r $DATA_WORKING_DIR/*

wget -O /dev/null "$SOLR_URL$SOLR_COLLECTION/update?commit=true&stream.body=<delete><query>*:*</query></delete>"
wget -O /dev/null "$SOLR_URL$SOLR_TOPIC_COLLECTION/update?commit=true&stream.body=<delete><query>*:*</query></delete>"

../neo4j/bin/neo4j stop
rm -r ../neo4j/data/databases/graph.db/
../neo4j/bin/neo4j start


#url $SOLR/update\?commit\=true -d  '<delete><query>*:*</query></delete>' || true
#curl $SOLR_TOPICS/update\?commit\=true -d  '<delete><query>*:*</query></delete>' || true
#curl -H "Content-Type: application/json" -X POST \
#     -d '{"statements": [{"statement": "MATCH (n) DETACH DELETE n"}]}' \
#     "http://"$NEO4J_HOST":"$NEO4J_HTTP_PORT"/db/data/transaction/commit" || true
#curl -H "Content-Type: application/json" -X POST \
#     -d '{"statements": [{"statement": "DROP INDEX ON :Person(identifying_name)"}]}' \
#     "http://"$NEO4J_HOST":"$NEO4J_HTTP_PORT"/db/data/transaction/commit" || true
