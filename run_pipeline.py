"""This module runs the main pipeline."""

import argparse
import ujson as json

from src.util import get_config
from src.common import Pipeline, SparkProvider
from src.reader import EmlReader
from src.preprocessing import EmailDecoding, EmailSplitting, HeaderParsing, TextCleaning, LanguageDetection
from src.deduplication import EmailDeduplication
from src.ner import SpacyNer
from src.topics import TopicModelPrediction, TopicModelTraining
from src.writer import TextFileWriter, SolrFileWriter
from src.category_classification import EmailCategoryClassification
from src.folder_classification import EmailFolderClassification
from src.network_analysis import NetworkAnalyser


def run_email_pipeline(read_from, write_to, solr, solr_url,
                       neo4j_host, neo4j_http_port, neo4j_bolt_port, analyse_network,
                       dataset):
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
        TopicModelPrediction(),
        LanguageDetection(read_from='text_clean'),
        SpacyNer(read_from='text_clean'),
        EmailCategoryClassification(),
        EmailFolderClassification()
    ]

    writer = TextFileWriter(path=write_to)

    Pipeline(reader, pipes, writer).run()

    if solr:
        SolrFileWriter(write_to, solr_url=solr_url).run()

    SparkProvider.stop_spark_context()

    if analyse_network:
        network_analyser = NetworkAnalyser(host=neo4j_host, http_port=neo4j_http_port, bolt_port=neo4j_bolt_port)
        network_analyser.analyse_network(upload=False)


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
    parser.add_argument('--neo4j-host',
                        help='Neo4j host to get network data from.',
                        default='sopedu.hpi.uni-potsdam.de')
    parser.add_argument('--neo4j_http_port',
                        help='Neo4j http port to use for http connections.',
                        default='7474')
    parser.add_argument('--neo4j_bolt_port',
                        help='Neo4j bolt port to use for bolt connections.',
                        default='7687')
    parser.add_argument('--analyse_network',
                        action='store_true',
                        help='Set if the network data should be analysed.')
    parser.add_argument('--dataset',
                        help='Dataset config to read.')
    args = parser.parse_args()

    run_email_pipeline(args.read_from,
                       args.write_to,
                       args.solr,
                       args.solr_url,
                       args.neo4j_host,
                       args.neo4j_http_port,
                       args.neo4j_bolt_port,
                       args.analyse_network,
                       args.dataset)
