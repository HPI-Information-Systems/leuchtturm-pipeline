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
(~/pipeline)$ export LEUCHTTURM_RUNNER=LOCAL  # only if you're not on a cluster
(~/pipeline)$ python run_pipeline.py  # this runs the pipeline with default params
(~/pipeline)$ python run_pipeline.py --help  # to explore config options (such as solr upload, custom paths, ...)
```

Check Solr or the `pipeline_result` folder for the results! To deploy the pipeline on a yarn cluster, consult [this Guide](https://hpi.de/naumann/leuchtturm/gitlab/leuchtturm/meta/wikis/Pipeline/Pipeline-Architektur).
