# Leuchtturm Pipeline

This repository contains all code to process emails.

## Getting started:

Requirements:

Make sure you have `Python 3` and `Spark 2.2` installed on your machine.

```bash
# create a python environment (optional) and install all requirements
(~/pipeline)$ conda create -n leuchtturm python=3.6 && source activate leuchtturm
(~/pipeline)$ pip install -r requirements.txt
```

## Running a pipeline

Crowd the `emails` folder with some fishy emails (one file for each email, `RFC 822` compliant as e.g. exports from Thunderbird or Apple Mail).

And you're ready to go!

```bash
export LEUCHTTURM=RUNNER=LOCAL  # to run in debug mode with limited num of partitions
# start the email pipeline
(~/pipeline)$ python src/run_pipeline.py --read-from ./emails --write-to ./tmp
# or start pipeline with upload to solr
(~/pipeline)$ python src/run_pipeline.py --read-from ./emails --write-to ./tmp --solr --solr-url http://where/yor/solr/is:running
```

Check Solr or the `tmp` folder for the results! To deploy the pipeline on a yarn cluster, consult [this Guide](https://hpi.de/naumann/leuchtturm/gitlab/leuchtturm/meta/wikis/Pipeline/Pipeline-Architektur).

## Pipeline tasks

- read `.eml` compliant files
- decode multipart mime emails
- splitting
- metadata extraction
- deduplication
- cleaning of text (for further text mining tasks)
- language detection
- entity extraction
- write to solr db
- write to neo4j db
