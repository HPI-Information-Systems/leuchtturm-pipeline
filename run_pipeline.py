# flake8: noqa
"""This module runs the main pipeline."""

import ujson as json
from datetime import datetime

from config.config import Config
from src.common import Pipeline, SparkProvider
from src.reader import EmlReader, TextFileReader
from src.preprocessing import EmailDecoding, EmailSplitting, HeaderParsing, TextCleaning, LanguageDetection
from src.deduplication import EmailDeduplication
from src.ner import SpacyNer
from src.topics import TopicModelTraining, TopicModelPrediction
from src.writer import TextFileWriter, SolrFileWriter, Neo4JFileWriter
from src.signature_extraction import SignatureExtraction
from src.correspondent_extraction_aggregation \
    import CorrespondentDataExtraction, CorrespondentDataAggregation, CorrespondentIdInjection
from src.category_classification import EmailCategoryClassification
from src.folder_classification import EmailFolderClassification


def run_email_pipeline(conf):
    """Run main email pipeline."""
    SparkProvider.spark_context(conf)

    reader = EmlReader(conf, conf.get('data', 'source_dir'))
    pipes = [
        EmailDecoding(conf, split_header_body=False),
        EmailSplitting(conf, keep_thread_connected=True),
        HeaderParsing(conf, use_unix_time=False),
        EmailDeduplication(conf, is_connected_thread=True),
        TextCleaning(conf, read_from='body', write_to='text_clean', write_to_original_ws='text_clean_original_ws'),
        SignatureExtraction(  # also relies on document['header']['sender']['email']
            conf,
            read_from='text_clean_original_ws',
            write_body_without_signature_to='body_without_signature',
            write_signature_to='signature'
        ),
        LanguageDetection(conf, read_from='text_clean'),
        SpacyNer(conf, read_from='text_clean'),
        EmailCategoryClassification(conf)
    ]

    writer = TextFileWriter(conf, path=conf.get('data', 'results_dir'))
    Pipeline(reader, pipes, writer).run()

    if conf.get('topic_modelling', 'train_model'):
        run_topic_model_training(conf)

    reader = TextFileReader(conf, path=conf.get('data', 'results_dir'))
    writer = TextFileWriter(conf, path=conf.get('topic_modelling', 'working_dir'))
    Pipeline(reader, [TopicModelPrediction(conf)], writer).run()

    reader = TextFileReader(conf, path=conf.get('data', 'results_dir'))
    pipes = [
        CorrespondentDataExtraction(conf),
        CorrespondentDataAggregation(conf),
    ]
    writer = TextFileWriter(conf, path=conf.get('data', 'results_correspondent_dir'))
    Pipeline(reader, pipes, writer).run()

    correspondent_rdd = SparkProvider.spark_context(conf).broadcast(
        TextFileReader(conf, path=conf.get('data', 'results_correspondent_dir')).run().collect()
    )
    pipes = [
        CorrespondentIdInjection(conf, correspondent_rdd),
    ]
    writer = TextFileWriter(conf, path=conf.get('data', 'results_injected_dir'))
    Pipeline(reader, pipes, writer).run()

    if conf.get('solr', 'import'):
        SolrFileWriter(conf,
                       conf.get('data', 'results_injected_dir'),
                       conf.solr_url + conf.get('solr', 'collection')).run()
        SolrFileWriter(conf,
                       conf.get('topic_modelling', 'working_dir'),
                       conf.solr_url + conf.get('solr', 'topic_collection')).run()

    if conf.get('neo4j', 'import'):
        Neo4JFileWriter(conf, conf.get('data', 'results_correspondent_dir'), mode='nodes').run()
        Neo4JFileWriter(conf, conf.get('data', 'results_injected_dir'), mode='edges').run()

    SparkProvider.stop_spark_context()


def run_topic_model_training(conf):
    """Run tm training on small datasets."""
    # FIXME use existing pipeline output for this to get clean bodies!
    df = EmlReader(conf, conf.get('data', 'source_dir')).run()
    df = EmailDecoding(conf, split_header_body=False).run(df)
    df = EmailSplitting(conf, keep_thread_connected=False).run(df)
    df = TextCleaning(conf, read_from='body', write_to='text_clean').run(df).map(lambda x: json.loads(x)['text_clean'])
    TopicModelTraining(conf).run(df)


if __name__ == '__main__':
    conf = Config()
    run_email_pipeline(conf)
