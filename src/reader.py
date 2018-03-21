"""Entry points to leuchtturm pipelines.

Read documents from different sources and transform them into leuchtturm format.
Leuchtturm format is a Spark RDD where each line represents a document.
Resulting documents must be in json format with at least these fields:
    {
        'doc_id': 'something_unique',
        'path': 'path/to/original/file',
        'raw': 'Raw content of file.'
    }
"""

import email
import ujson as json
import uuid

from common import SparkProvider


def eml_reader(path, filename_is_doc_id=False, apply_email_filter=True):
    """Read emails from a directory.

    Each raw email is stored in a separate file.
    Will be represented as a json object including basic metainfo in rdd.
    """
    def is_valid_email(document):
        """Return true if string is a RFC822 compliant email."""
        return len(email.message_from_string(document).defects) == 0

    def create_document(document, path):
        """Create json entry for a document."""
        doc_id = path.split('/')[-1].split('.') if filename_is_doc_id else str(uuid.uuid4())

        return json.dumps({'doc_id': doc_id,
                           'path': path,
                           'raw': document})

    rdd = SparkProvider.spark_context().wholeTextFiles(path,
                                                       minPartitions=SparkProvider.spark_parallelism())
    rdd = rdd.filter(lambda x: is_valid_email(x[1])) if apply_email_filter else rdd
    rdd = rdd.map(lambda x: create_document(x[1], x[0]))

    return rdd


def textfile_reader(path):
    """Read rdd that has been exported using saveAsTextfile.

    Expect json format as specified.
    No transformations will be applied.
    """
    return SparkProvider.spark_context().textFile(path,
                                                  minPartitions=SparkProvider.spark_parallelism())


def csv_reader():
    """Read emails from a csv file where each raw email is located in a column of a line."""
    raise NotImplementedError


def mbox_reader():
    """Read emails from a mbox archive."""
    raise NotImplementedError
