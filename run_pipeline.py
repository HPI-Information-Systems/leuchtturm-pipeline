"""This module runs the main pipeline."""

import argparse
import ujson as json

from src.util import get_config
from src.common import Pipeline, SparkProvider
from src.reader import EmlReader
from src.preprocessing import EmailDecoding, HeaderParsing, TextCleaning, LanguageDetection
from src.deduplication import EmailDeduplication
from src.ner import SpacyNer
from src.topics import TopicModelPrediction, TopicModelTraining
from src.writer import TextFileWriter, SolrFileWriter


def run_email_pipeline(read_from='./emails', write_to='./pipeline_result',
                       solr=False, solr_url='http://sopedu.hpi.uni-potsdam.de:8983/solr/enron',
                       dataset='enron'):
    """Run main email pipeline."""
    config = get_config(dataset)
    SparkProvider.spark_context()

    reader = EmlReader(read_from)

    pipes = [EmailDecoding(split_header_body=True),
             HeaderParsing(config, clean_subject=False, use_unix_time=False),
             EmailDeduplication(),
             TextCleaning(read_from='body', write_to='text_clean'),
             TopicModelPrediction(),
             LanguageDetection(read_from='text_clean'),
             SpacyNer(read_from='text_clean')]

    writer = TextFileWriter(path=write_to)

    Pipeline(reader, pipes, writer).run()

    if solr:
        SolrFileWriter(write_to, solr_url=solr_url).run()

    SparkProvider.stop_spark_context()


def run_topic_model_training():
    """Run tm training on small datasets."""
    df = EmlReader('./emails').run()
    df = HeaderParsing().run(df)
    df = TextCleaning(read_from='body', write_to='text_clean').run(df).map(lambda x: json.loads(x)['text_clean'])
    TopicModelTraining().run(df)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--read-from',
                        help='Path to directory where raw emails are located.',
                        default='./emails')
    parser.add_argument('--write-to',
                        help='Path where results will be written to. Must not yet exist.',
                        default='./pipeline_result')
    parser.add_argument('--solr',
                        action='store_true',
                        help='Set if results should be written to solr.')
    parser.add_argument('--solr-url',
                        help='Url to running solr instance (core/collection specified).',
                        default='http://sopedu.hpi.uni-potsdam.de:8983/solr/enron')
    parser.add_argument('--dataset',
                        help='Dataset config to read.',
                        default='enron')
    args = parser.parse_args()

    run_email_pipeline(read_from=args.read_from,
                       write_to=args.write_to,
                       solr=args.solr,
                       solr_url=args.solr_url,
                       dataset=args.dataset)
