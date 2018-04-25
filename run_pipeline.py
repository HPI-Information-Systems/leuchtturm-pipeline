"""This module runs the main pipeline."""

import argparse
import ujson as json

from src.util import get_config
from src.common import Pipeline, SparkProvider
from src.reader import EmlReader, TextFileReader
from src.preprocessing import EmailDecoding, EmailSplitting, HeaderParsing, TextCleaning, LanguageDetection
from src.deduplication import EmailDeduplication
from src.ner import SpacyNer
from src.topics import TopicModelPrediction, TopicModelTraining
from src.writer import TextFileWriter, SolrFileWriter
from src.signature_extraction import SignatureExtraction
from src.correspondent_extraction_aggregation \
    import CorrespondentDataExtraction, CorrespondentDataAggregation, CorrespondentIdInjection
from src.category_classification import EmailCategoryClassification
from src.folder_classification import EmailFolderClassification


def run_email_pipeline(read_from, write_to, solr, solr_url, dataset):
    """Run main email pipeline."""
    config = get_config(dataset)
    SparkProvider.spark_context()

    reader = EmlReader(read_from)
    pipes = [
        EmailDecoding(split_header_body=False),
        EmailSplitting(keep_thread_connected=True),
        HeaderParsing(config=config, use_unix_time=False),
        EmailDeduplication(is_connected_thread=True),
        TextCleaning(read_from='body', write_to='text_clean'),
        SignatureExtraction(  # also relies on document['header']['sender']['email']
            read_from='body',
            write_body_without_signature_to='body_without_signature',
            write_signature_to='signature'
        ),
        TopicModelPrediction(),
        LanguageDetection(read_from='text_clean'),
        SpacyNer(read_from='text_clean'),
        EmailCategoryClassification(),
        EmailFolderClassification()
    ]
    writer = TextFileWriter(path=write_to)
    Pipeline(reader, pipes, writer).run()

    reader = TextFileReader(write_to)
    pipes = [
        CorrespondentDataExtraction(),
        CorrespondentDataAggregation(),
    ]
    writer = TextFileWriter(path=write_to + '_correspondent')
    Pipeline(reader, pipes, writer).run()

    correspondent_rdd = SparkProvider.spark_context().broadcast(
        TextFileReader(write_to + '_correspondent').run().collect()
    )
    pipes = [
        CorrespondentIdInjection(correspondent_rdd),
    ]
    writer = TextFileWriter(path=write_to + '_injected')
    Pipeline(reader, pipes, writer).run()

    # Neo4JFileWriter(write_to + '_correspondent').run()

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
                        help='Set this flag if results should be written to solr.')
    parser.add_argument('--solr-url',
                        help='Url to running solr instance (with core/collection specified).',
                        default='http://sopedu.hpi.uni-potsdam.de:8983/solr/enron')
    parser.add_argument('--dataset',
                        help='Dataset config to read.')
    args = parser.parse_args()

    run_email_pipeline(args.read_from,
                       args.write_to,
                       args.solr,
                       args.solr_url,
                       args.dataset)
