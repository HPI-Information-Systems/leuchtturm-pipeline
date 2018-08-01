# flake8: noqa
"""This module runs the main pipeline."""

import ujson as json

from config.config import Config
from src.common import Pipeline, SparkProvider
from src.reader import EmlReader, TextFileReader
from src.preprocessing import EmailDecoding, EmailSplitting, HeaderParsing, TextCleaning, LanguageDetection
from src.deduplication import EmailDeduplication
from src.ner import SpacyNer
from src.phrase_detection import PhraseDetection
from src.topics import TopicModelPreprocessing, TopicModelTraining, TopicModelTrainingOld, TopicModelPrediction, TopicSimilarity
from src.writer import TextFileWriter, SolrFileWriter, Neo4JFileWriter
from src.signature_extraction import SignatureExtraction
from src.correspondent_extraction_aggregation \
    import CorrespondentDataExtraction, CorrespondentDataAggregation, CorrespondentIdInjection
from src.category_classification import EmailCategoryClassification
from src.cluster_prediction import EmailClusterPrediction
from src.network_analysis import NetworkAnalyser
from src.network_uploader import NetworkUploader


def run_email_pipeline(conf):
    """Run main email pipeline."""
    SparkProvider.spark_context(conf)

    reader = EmlReader(conf, conf.get('data', 'source_dir'))
    pipes = [
        EmailDecoding(conf, split_header_body=False),
        EmailSplitting(conf, keep_thread_connected=True),
        HeaderParsing(conf, use_unix_time=False),
        EmailDeduplication(conf, is_connected_thread=True),
        TextCleaning(conf, read_from='body', write_to='body', write_to_original_ws='body_original_ws'),
        SignatureExtraction(  # also relies on document['header']['sender']['email']
            conf,
            read_from='body_original_ws',
            write_body_without_signature_to='body_without_signature',
            write_signature_to='signature'
        ),
        PhraseDetection(conf, read_from='body_without_signature'),
        LanguageDetection(conf, read_from='body'),
        EmailCategoryClassification(conf),
        EmailClusterPrediction(conf),
        TopicModelPreprocessing(conf, read_from='body_without_signature', write_to='bow'),
    ]
    writer = TextFileWriter(conf, path=conf.get('data', 'results_dir'))
    results_rdd = Pipeline(reader, pipes, writer).run()

    topic_model, topic_dictionary = TopicModelTraining(conf, read_from='bow').run(results_rdd)
    topic_model_broadcast = SparkProvider.spark_context(conf).broadcast(topic_model)
    topic_dictionary_broadcast = SparkProvider.spark_context(conf).broadcast(topic_dictionary)

    topic_ranks = TopicSimilarity(conf, model=topic_model_broadcast).run()

    reader = TextFileReader(conf, path=conf.get('data', 'results_dir'))
    pipes = [
        TopicModelPrediction(conf, topic_ranks=topic_ranks, read_from='bow', model=topic_model_broadcast, dictionary=topic_dictionary_broadcast)
    ]
    writer = TextFileWriter(conf, path=conf.get('data', 'results_topics_dir'))
    Pipeline(reader, pipes, writer).run()

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
                       conf.get('data', 'results_topics_dir'),
                       conf.solr_url + conf.get('solr', 'topic_collection')).run()

    if conf.get('neo4j', 'import'):
        Neo4JFileWriter(conf, conf.get('data', 'results_correspondent_dir'), mode='nodes').run()
        Neo4JFileWriter(conf, conf.get('data', 'results_injected_dir'), mode='edges').run()
        if conf.get('network_analysis', 'run'):
            NetworkAnalyser(conf).run()
            NetworkUploader(conf).run()

    SparkProvider.stop_spark_context()


def run_topic_model_training(conf):
    """Run tm training on small datasets."""
    # FIXME use existing pipeline output for this to get clean bodies!
    df = EmlReader(conf, conf.get('data', 'source_dir')).run()
    df = EmailDecoding(conf, split_header_body=False).run(df)
    df = EmailSplitting(conf, keep_thread_connected=False).run(df)
    df = TextCleaning(conf, read_from='body', write_to='text_clean', readable=False).run(df).map(lambda x: json.loads(x)['text_clean'])
    TopicModelTrainingOld(conf).run(df)


if __name__ == '__main__':
    conf = Config()
    run_email_pipeline(conf)
