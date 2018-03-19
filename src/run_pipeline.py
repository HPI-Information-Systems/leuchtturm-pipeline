"""This module runs the main pipeline."""

import argparse

from common import Pipeline, SparkProvider
from reader import EmlReader
from preprocessing import HeaderBodyParsing, TextCleaning, LanguageDetection
from deduplication import EmailDeduplication
from ner import SpacyNer
from topics import TopicModelPrediction
from writer import TextfileWriter, SolrWriter


def run_email_pipeline(read_from='./emails', write_to='./pipeline_result',
                       solr=False, solr_url='http://0.0.0.0:8983/solr/enron'):
    """Run main email pipeline."""
    SparkProvider.spark_context()

    reader = EmlReader(read_from)

    pipes = [HeaderBodyParsing(clean_subject=False, use_unix_time=True),
             EmailDeduplication(use_metadata=True),
             TextCleaning(read_from='body', write_to='text_clean'),
             TopicModelPrediction(),
             LanguageDetection(read_from='text_clean'),
             SpacyNer(read_from='text_clean')]

    writer = TextfileWriter(path=write_to) if not solr else SolrWriter(solr_url=solr_url)

    Pipeline(reader, pipes, writer).run()

    SparkProvider.stop_spark_context()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--read_from',
                        help='Path to directory where raw emails are located.',
                        default='./emails')
    parser.add_argument('--write_to',
                        help='Path where results will be written to. Must not yet exist.',
                        default='./pipeline_result')
    parser.add_argument('--solr',
                        action='store_true',
                        help='Set if results should be written to solr.')
    parser.add_argument('--solr_url',
                        help='Url to running solr instance (core/collection specified).',
                        default='http://0.0.0.0:8983/solr/enron')
    args = parser.parse_args()

    run_email_pipeline(read_from=args.read_from, write_to=args.write_to, solr=args.solr, solr_url=args.solr_url)
