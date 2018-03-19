"""This module runs pipeline tasks in correct order."""

from common import Pipeline, SparkProvider
from reader import EmlReader
from preprocessing import HeaderBodyParsing, TextCleaning, LanguageDetection
from deduplication import EmailDeduplication
from ner import SpacyNer
from topics import TopicModelPrediction
from writer import TextfileWriter


def run_email_pipeline():
    """Run main email pipeline."""
    SparkProvider.spark_context()

    reader = EmlReader('./emails')

    pipes = [HeaderBodyParsing(clean_subject=False, use_unix_time=True),
             EmailDeduplication(use_metadata=True),
             TextCleaning(read_from='body', write_to='text_clean'),
             TopicModelPrediction(),
             LanguageDetection(read_from='text_clean'),
             SpacyNer(read_from='text_clean')]

    writer = TextfileWriter(path='./tmp')

    Pipeline(reader, pipes, writer).run()

    SparkProvider.stop_spark_context()


if __name__ == '__main__':
    run_email_pipeline()
