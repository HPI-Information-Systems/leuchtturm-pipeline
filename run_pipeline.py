"""This module runs the main pipeline."""

import ujson as json

from config.config import Config
from src.common import Pipeline, SparkProvider
from src.reader import EmlReader, TextFileReader
from src.preprocessing import EmailDecoding, EmailSplitting, HeaderParsing, TextCleaning, LanguageDetection
from src.deduplication import EmailDeduplication
from src.ner import SpacyNer
from src.topics import TopicModelPrediction, TopicModelTraining, TopicModelPreprocessing, TopicModelBucketing
from src.writer import TextFileWriter, SolrFileWriter
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
        TextCleaning(conf, read_from='body', write_to='text_clean'),
        LanguageDetection(conf, read_from='text_clean'),
        SpacyNer(conf, read_from='text_clean'),
        EmailCategoryClassification(conf),
        EmailFolderClassification(conf)
    ]

    writer = TextFileWriter(conf, path=conf.get('data', 'results_dir'))
    Pipeline(reader, pipes, writer).run()

    if conf.get('topic_modelling', 'train_model'):
        reader = TextFileReader(conf, path=conf.get('data', 'results_dir'))
        pipes = [
            TopicModelBucketing(conf)
        ]
        writer = TextFileWriter(conf, path=conf.get('tm_preprocessing', 'buckets_dir'))
        Pipeline(reader, pipes, writer).run()

        run_topic_model_training(conf)

    reader = TextFileReader(conf, path=conf.get('data', 'results_dir'))
    pipes = [
        TopicModelPreprocessing(conf, read_from='text_clean', write_to='bow'),
        TopicModelPrediction(conf)
    ]
    writer = TextFileWriter(conf, path=conf.get('topic_modelling', 'working_dir'))
    Pipeline(reader, pipes, writer).run()

    if conf.get('neo4j', 'import'):
        # TODO implement neo4j import
        pass

    if conf.get('solr', 'import'):
        SolrFileWriter(conf,
                       conf.get('data', 'results_dir'),
                       conf.solr_url + conf.get('solr', 'collection')).run()
        SolrFileWriter(conf,
                       conf.get('topic_modelling', 'working_dir'),
                       conf.solr_url + conf.get('solr', 'topic_collection')).run()

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
