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

Crowd the `emails` folder with some fishy emails (one file for each email, `RFC 822` compliant as e.g. exports from Thunderbird or Apple Mail).

And you're ready to go!

```bash
# start the email pipeline
(~/pipeline)$ python src/file_lister.py
(~/pipeline)$ python src/run_leuchtturm.py
(~/pipeline)$ python src/write_to_solr.py  # if you have a running solr instance
```

Check Solr or the `tmp` folder for the results! To deploy the pipeline on a yarn cluster, read [this](https://hpi.de/naumann/leuchtturm/gitlab/leuchtturm/meta/wikis/Pipeline/Pipeline-Architektur).


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


## Project structure

```bash
├── emails                      <-- here, your raw emails go
│   └── *.eml
├── models                      <-- assets for pipeline tasks
│   ├── lda_model.pickle
│   └── *
├── src                         <-- tasks and pipeline definitions
│   ├── utils
│   │   ├── libs*.py
│   ├── run_leuchtturm.py
│   ├── run_leuchtturm.py
│   ├── write_to_solr.py
│   ├── write_to_neo4j.py
│   └── *.py
├── tests                       <-- tests for each module
│   ├── test_file_lister.py
│   ├── test_leuchtturm.py
│   └── test_*.py
├── temp                        <-- default path for pipeline output
│   ├── files_listed
│   └── pipeline_results
├── README.md
├── *.sh                        <-- deployment scripts
└── *
```