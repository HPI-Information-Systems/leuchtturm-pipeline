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
# start the email pipeline
(~/pipeline)$ python src/file_lister.py ./emails ./tmp/fl
(~/pipeline)$ python src/run_leuchtturm.py ./tmp/fl ./tmp/pr
(~/pipeline)$ python src/write_to_solr.py  ./tmp/pr http://0.0.0.0:8983/solr/emails  # if you have a running solr instance
```

Check Solr or the `tmp` folder for the results! To deploy the pipeline on a yarn cluster, consult [this Guide](https://hpi.de/naumann/leuchtturm/gitlab/leuchtturm/meta/wikis/Pipeline/Pipeline-Architektur).

## Pipeline tasks

- read `.eml` compliant files
- splitting
- metadata extraction
- deduplication
- cleaning of text (for further text mining tasks)
- language detection
- entity extraction
- write to solr db
- write to neo4j db
