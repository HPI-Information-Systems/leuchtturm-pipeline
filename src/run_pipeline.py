"""This module runs the main pipeline."""

import argparse

from common import SparkProvider
from reader import eml_reader, textfile_reader
from preprocessing import decode_mime_email, header_parsing, text_cleaning, language_detection
from deduplication import email_deduplication
from topics import topic_model_prediction
from ner import spacy_ner
from writer import solr_writer, textfile_writer


def run_email_pipeline(read_from='./emails', write_to='./pipeline_result',
                       solr=False, solr_url='http://0.0.0.0:8983/solr/enron'):
    """Run main email pipeline."""
    SparkProvider.spark_context()

    # main pipeline
    emails = eml_reader(read_from)

    emails = decode_mime_email(emails)
    emails = header_parsing(emails, clean_subject=False, use_unix_time=False)
    emails = email_deduplication(emails)
    emails = text_cleaning(emails, read_from='body', write_to='text_clean')
    emails = topic_model_prediction(emails)
    emails = language_detection(emails, read_from='text_clean')
    emails = spacy_ner(emails, read_from='text_clean')

    textfile_writer(emails, path=write_to)

    # db upload pipelines
    if solr:
        emails = textfile_reader(write_to)
        solr_writer(emails, solr_url=solr_url)

    SparkProvider.stop_spark_context()


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
                        default='http://0.0.0.0:8983/solr/enron')
    args = parser.parse_args()

    run_email_pipeline(read_from=args.read_from, write_to=args.write_to, solr=args.solr, solr_url=args.solr_url)
