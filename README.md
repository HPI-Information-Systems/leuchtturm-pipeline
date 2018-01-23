# Leuchtturm Pipeline

This repository contains all code to read, process and export emails.

## Getting started:

Requirements:

- python3 and pip3
- spark2.2.0

On every cluster node, run once:

```bash
pip3 install -r requirements.txt
spacy download en_core_web_sm
```

To execute a task, run on one cluster node:
```bash
export SPARK_HOME={path-to-your-spark2-home}
export PYSPARK_PYTHON=python3
spark-submit --master yarn \
             --deploy-mode cluster \
             --driver-memory {set recommended} \
             --executor-memory {set recommended} \
             --num-executors {set recommended} \
             --executor-cores {set recommended} \
             --py-files {dependent .py files (e.g. leuchtturm.py} \
             {.py file you want to run (e.g. run_leuchtturm.py)}
```

Track your application's progress here: http://b7689.byod.hpi.de:8088/cluster


## Pipeline

### Preprocessing

`file_lister.py`

Read all emails of a directory, and save them to a Spark RDD.
Emails must be in `RFC 822` format (e.g. an `.eml` file or a `Nuix`-Export).
Every line in the resulting RDD represents one email of the directory. Lines are `JSON` compliant.


### Pipeline

`run_leuchtturm.py` and `leuchtturm.py`

Read the prepared RDD, and process defined tasks on the contained emails:

- splitting
- metadata extraction
- deduplication
- cleaning of text (for further text mining tasks)
- language detection
- entity extraction

Resulting file is a dumped RDD with `JSON` compliant lines.


### DB tasks

`write_to_solr.py` and `write_to_neo4j.py`

Read the processed RDD and write it to connected databases, such as *Solr* or *Neo4J*.
