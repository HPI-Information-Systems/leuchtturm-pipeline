"""This module runs the main pipeline."""
import ujson as json

from src.common import Pipeline, SparkProvider
from src.reader import EmlReader, TextFileReader
from src.preprocessing import EmailDecoding, EmailSplitting, HeaderParsing, TextCleaning, LanguageDetection
from src.deduplication import EmailDeduplication
from src.ner import SpacyNer
from src.topics import TopicModelPrediction, TopicModelTraining
from src.writer import TextFileWriter, SolrFileWriter
from src.category_classification import EmailCategoryClassification
from src.folder_classification import EmailFolderClassification
from config.config import Config


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
    writer = TextFileWriter(conf, path=conf.get('data', 'working_dir'))
    Pipeline(reader, pipes, writer).run()

    if conf.get('topic_modelling', 'train_model'):
        run_topic_model_training(conf)

    reader = TextFileReader(conf, path=conf.get('data', 'working_dir'))
    writer = TextFileWriter(conf, path=conf.get('topic_modelling', 'working_dir'))
    Pipeline(reader, [TopicModelPrediction(conf)], writer).run()

    if conf.get('neo4j', 'import'):
        # TODO implement neo4j import
        pass

    if conf.get('solr', 'import'):
        SolrFileWriter(conf, conf.get('data', 'working_dir'), conf.get('solr', 'collection')).run()
        SolrFileWriter(conf, conf.get('topic_modelling', 'working_dir'), conf.get('solr', 'topic_collection')).run()

    SparkProvider.stop_spark_context()


def run_topic_model_training(conf):
    """Run tm training on small datasets."""
    # FIXME use existing pipeline output for this to get clean bodies!
    df = EmlReader(conf, conf.get('data', 'source_dir')).run()
    df = HeaderParsing(conf).run(df)
    df = TextCleaning(conf, read_from='body', write_to='text_clean').run(df).map(lambda x: json.loads(x)['text_clean'])
    TopicModelTraining(conf).run(df)


if __name__ == '__main__':
    conf = Config()
    run_email_pipeline(conf)
