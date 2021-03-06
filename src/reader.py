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
import os

from .common import Pipe, SparkProvider


class EmlReader(Pipe):
    """Read emails from a directory.

    Each raw email is stored in a separate file.
    Will be represented as a json object including basic metainfo in rdd.
    """

    def __init__(self, conf, source_directory, filename_is_doc_id=False, apply_email_filter=True):
        """Set params. Can generate uuids if filenames are not unique and drop all non-emails."""
        super().__init__(conf)
        self.conf = conf
        self.source_directory = source_directory
        self.filename_is_doc_id = filename_is_doc_id
        self.apply_email_filter = apply_email_filter

    def is_valid_email(self, x):
        """Return true if string is a RFC822 compliant email."""
        ret = x[0].endswith('.eml') and len(email.message_from_string(x[1]).defects) == 0
        if not ret:
            print(' > ignoring {}'.format(x[0]), flush=True)
        return ret

    def create_document(self, document, path):
        """Create json entry for a document."""
        doc_id = path.split(os.sep)[-1].split('.')[0] if self.filename_is_doc_id else str(uuid.uuid4())
        return json.dumps({'doc_id': doc_id,
                           'path': path,
                           'raw': document}, ensure_ascii=False)

    def run(self):
        """Run task in a spark context. Return rdd."""
        rdd = SparkProvider.spark_context(self.conf).wholeTextFiles(self.source_directory,
                                                                    minPartitions=self.parallelism)
        rdd = rdd.filter(lambda x: self.is_valid_email(x)) if self.apply_email_filter else rdd
        rdd = rdd.map(lambda x: self.create_document(x[1], x[0]))

        return rdd


class TextFileReader(Pipe):
    """Read rdd that has been exported using saveAsTextfile.

    Expect json format as specified.
    No transformations will be applied.
    """

    def __init__(self, conf, path):
        """Set params. path is location of dumped rdd (local or hdfs)."""
        super().__init__(conf)
        self.conf = conf
        self.path = path

    def run(self):
        """Run task in spark context."""
        return SparkProvider.spark_context(self.conf).textFile(self.path, minPartitions=self.parallelism)


class CsvReader(Pipe):
    """Read emails from a csv file where each raw email is located in a column of a line."""

    def __init__(self):
        """IMPLEMENT ME."""
        raise NotImplementedError


class MboxReader(Pipe):
    """Read emails from a mbox archive."""

    def __init__(self):
        """IMPLEMENT ME."""
        raise NotImplementedError
