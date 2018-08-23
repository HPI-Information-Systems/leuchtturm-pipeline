# flake8: noqa
"""This module runs the main pipeline."""

from config.config import Config
from src.common import Pipeline, SparkProvider
from src.reader import EmlReader, TextFileReader
from src.preprocessing import EmailDecoding, EmailSplitting, HeaderParsing, TextCleaning, LanguageDetection, DummyValues
from src.deduplication import EmailDeduplication
from src.phrase_detection import PhraseDetection
from src.topics import TopicModelPreprocessing, TopicModelTraining, TopicModelPrediction, TopicSimilarity
from src.writer import TextFileWriter, SolrFileWriter, Neo4JFileWriter  # , CSVGraphWriter
from src.signature_extraction import SignatureExtraction
from src.correspondent_extraction_aggregation \
    import CorrespondentDataExtraction, CorrespondentDataAggregation, CorrespondentIdInjection
from src.category_classification import EmailCategoryClassification
from src.cluster_prediction import EmailClusterPrediction
from src.network_analysis import NetworkAnalyser
from src.network_uploader import NetworkUploader
from configparser import NoOptionError
import warnings
import shutil
import os


def run_email_pipeline(conf):
    """Run main email pipeline."""
    SparkProvider.spark_context(conf)

    rdd = None

    if conf.get('stage_01', 'run'):
        print(' ##########################\n'
              ' # #      STAGE 1       # #\n'
              ' ##########################')
        rdd = EmlReader(conf, conf.get('stage_01', 'input')).run()
        rdd = EmailDecoding(conf, split_header_body=False).run(rdd)
        rdd = EmailSplitting(conf, keep_thread_connected=True).run(rdd)
        rdd = HeaderParsing(conf, use_unix_time=False).run(rdd)
        rdd = DummyValues(conf).run(rdd)
        if conf.get('stage_01', 'write'):
            TextFileWriter(conf, path=conf.get('stage_01', 'output')).run(rdd)

    if conf.get('stage_02', 'run'):
        print(' ##########################\n'
              ' # #      STAGE 2       # #\n'
              ' ##########################\n'
              ' # depends on stage 1')
        if rdd is None:
            rdd = TextFileReader(conf, path=conf.get('stage_01', 'input')).run()
        rdd = EmailDeduplication(conf, is_connected_thread=True).run(rdd)
        if conf.get('stage_02', 'write'):
            TextFileWriter(conf, path=conf.get('stage_02', 'output')).run(rdd)

    if conf.get('stage_03', 'run'):
        print(' ##########################\n'
              ' # #      STAGE 3       # #\n'
              ' ##########################\n'
              ' # depends on stage 1\n'
              ' # depends on stage 2 (optionally, strongly advised though)')
        if rdd is None:
            rdd = TextFileReader(conf, path=conf.get('stage_03', 'input')).run()
        rdd = TextCleaning(conf, read_from='body', write_to='body', write_to_original_ws='body_original_ws').run(rdd)
        rdd = SignatureExtraction(
            conf,
            # also relies on document['header']['sender']['email']
            read_from='body_original_ws',
            write_body_without_signature_to='body_without_signature',
            write_signature_to='signature'
        ).run(rdd)
        if conf.get('stage_03', 'write'):
            TextFileWriter(conf, path=conf.get('stage_03', 'output')).run(rdd)

    if conf.get('stage_04', 'run'):
        print(' ##########################\n'
              ' # #      STAGE 4       # #\n'
              ' ##########################\n'
              ' # depends on stage 1')
        if rdd is None:
            rdd = TextFileReader(conf, path=conf.get('stage_04', 'input')).run()
        rdd = PhraseDetection(conf, read_from='body_without_signature').run(rdd)
        rdd = LanguageDetection(conf, read_from='body').run(rdd)
        if conf.get('stage_04', 'write'):
            TextFileWriter(conf, path=conf.get('stage_04', 'output')).run(rdd)

    if conf.get('stage_05', 'run'):
        print(' ##########################\n'
              ' # #      STAGE 5       # #\n'
              ' ##########################\n'
              ' # depends on stage 1')
        if rdd is None:
            rdd = TextFileReader(conf, path=conf.get('stage_05', 'input')).run()
        rdd = EmailCategoryClassification(conf).run(rdd)
        if conf.get('stage_05', 'write'):
            TextFileWriter(conf, path=conf.get('stage_05', 'output')).run(rdd)

    if conf.get('stage_06', 'run'):
        print(' ##########################\n'
              ' # #      STAGE 6       # #\n'
              ' ##########################\n'
              ' # depends on stage 1')
        if rdd is None:
            rdd = TextFileReader(conf, path=conf.get('stage_06', 'input')).run()
        rdd = EmailClusterPrediction(conf).run(rdd)
        if conf.get('stage_06', 'write'):
            TextFileWriter(conf, path=conf.get('stage_06', 'output')).run(rdd)

    if conf.get('stage_07', 'run'):
        print(' ##########################\n'
              ' # #      STAGE 7       # #\n'
              ' ##########################\n'
              ' # depends on stage 1\n'
              ' # depends on stage 3')
        rdd_topics = TextFileReader(conf, path=conf.get('stage_07', 'input')).run()
        rdd_topics = TopicModelPreprocessing(conf, read_from='body_without_signature', write_to='bow').run(rdd_topics)

        topic_model, topic_dictionary = TopicModelTraining(conf, read_from='bow').run(rdd_topics)
        topic_model_broadcast = SparkProvider.spark_context(conf).broadcast(topic_model)
        topic_dictionary_broadcast = SparkProvider.spark_context(conf).broadcast(topic_dictionary)
        topic_ranks = TopicSimilarity(conf, model=topic_model_broadcast).run()

        rdd_topics = TopicModelPrediction(conf, topic_ranks=topic_ranks, read_from='bow', model=topic_model_broadcast,
                                          dictionary=topic_dictionary_broadcast).run(rdd_topics)

        if conf.get('stage_07', 'write'):
            TextFileWriter(conf, path=conf.get('stage_07', 'output')).run(rdd_topics)

    if conf.get('stage_08', 'run'):
        print(' ##########################\n'
              ' # #      STAGE 8       # #\n'
              ' ##########################\n'
              ' # depends on stage 1')
        rdd_correspondent = rdd
        if rdd_correspondent is None:
            rdd_correspondent = TextFileReader(conf, path=conf.get('stage_08', 'input')).run()
        rdd_correspondent = CorrespondentDataExtraction(conf).run(rdd_correspondent)
        rdd_correspondent = CorrespondentDataAggregation(conf).run(rdd_correspondent)
        if conf.get('stage_08', 'write'):
            TextFileWriter(conf, path=conf.get('stage_08', 'output')).run(rdd_correspondent)

    if conf.get('stage_09', 'run'):
        print(' ##########################\n'
              ' # #      STAGE 9       # #\n'
              ' ##########################\n'
              ' # depends on stage 1\n'
              ' # depends on stage 8')
        rdd_correspondent = SparkProvider.spark_context(conf).broadcast(
            TextFileReader(conf, path=conf.get('stage_09', 'input_1')).run().collect()
        )
        rdd_injected = rdd
        if rdd_injected is None:
            rdd_injected = TextFileReader(conf, path=conf.get('stage_09', 'input_2')).run()
        rdd_injected = CorrespondentIdInjection(conf, rdd_correspondent).run(rdd_injected)
        if conf.get('stage_09', 'write'):
            TextFileWriter(conf, path=conf.get('stage_09', 'output')).run(rdd_injected)

    if conf.get('solr', 'import'):
        print(' ##########################\n'
              ' # #    SOLR IMPORT     # #\n'
              ' ##########################')
        SolrFileWriter(conf, conf.get('solr', 'import_from_1'),
                       conf.solr_url + conf.get('solr', 'collection')).run()
        SolrFileWriter(conf, conf.get('solr', 'import_from_2'),
                       conf.solr_url + conf.get('solr', 'topic_collection')).run()

    if conf.get('neo4j', 'import'):
        print(' ##########################\n'
              ' # #   NEO4J IMPORT     # #\n'
              ' ##########################')
        Neo4JFileWriter(conf, conf.get('neo4j', 'import_from_1'), mode='nodes').run()
        Neo4JFileWriter(conf, conf.get('neo4j', 'import_from_2'), mode='edges').run()
        # CSVGraphWriter(conf).run()
        if conf.get('network_analysis', 'run'):
            NetworkAnalyser(conf).run()
            NetworkUploader(conf).run()

    SparkProvider.stop_spark_context()


def check_directory_existence(path):
    if os.path.isdir(path):
        print('WARNING: \n'
              ' You configured the system to write to an existing directory:\n'
              '  > {}'.format(path), flush=True)
        yn = input('Type \'y\' to delete the directory: ')
        if yn == 'y':
            shutil.rmtree(path)
            print(' -- done. directory deleted.', flush=True)
        else:
            print(' !! you have to !!', flush=True)
            exit(1)
    else:
        print(' > Directory checked, non-existent, good: {}'.format(path))


def warn(*args, **kwargs):
    pass


if __name__ == '__main__':
    # RuntimeWarning: numpy.ufunc size changed, may indicate binary incompatibility. Expected 192, got 176
    # warnings.filterwarnings("ignore", category=RuntimeWarning, module=importlib.get("__name__"))
    # RuntimeWarning: numpy.dtype size changed, may indicate binary incompatibility. Expected 96, got 88
    # warnings.filterwarnings("ignore", message="numpy.ufunc size changed")
    # warnings.simplefilter('ignore')
    warnings.filterwarnings('ignore', category=UserWarning)

    conf = Config()

    if not conf.get('settings', 'sklearn_warnings'):
        warnings.warn = warn

    print('Checking for existing data that might conflict with your config.')
    print('!! BE VERY CAREFUL !!')
    print(' ... when using ./clear.sh -c config/<your_config>.ini')
    print(' This may destroy existing results you didn\'t intend to delete!')
    print('!! BE VERY CAREFUL !!')
    for i in range(1, 10):
        stage = 'stage_{:02d}'.format(i)
        if conf.get(stage, 'run'):
            print('Stage "{}" active.'.format(stage), flush=True)
            for src in ['input_1', 'input_2', 'input']:
                try:
                    print(' > {}: {}'.format(src, conf.get(stage, src)))
                except NoOptionError:
                    pass
            if conf.get(stage, 'write'):
                print(' > Will write output.')
                check_directory_existence(conf.get(stage, 'output'))
            else:
                print(' > Will NOT write output.')
        else:
            print('Skipping stage "{}"'.format(stage), flush=True)

    print('\n'
          '\n'
          ' ############################# \n'
          ' You may need to clear your databases!\n'
          '   * SOLR (click following links to delete everything):\n'
          '     - {}{}/update?commit=true&stream.body=<delete><query>*:*</query></delete>\n'
          '     - {}{}/update?commit=true&stream.body=<delete><query>*:*</query></delete>\n'
          '\n'
          '   * NEO4J (execute in terminal):\n'
          '     $ neo4j/bin/neo4j stop\n'
          '     $ rm -r neo4j/data/databases/graph.db/\n'
          '     $ neo4j/bin/neo4j start\n'
          '\n'
          ' #############################\n'
          .format(conf.solr_url, conf.get('solr', 'collection'),
                  conf.solr_url, conf.get('solr', 'topic_collection')))

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        run_email_pipeline(conf)
