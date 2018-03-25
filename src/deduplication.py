"""Deduplication pipes for leuchtturm pipelines."""

import json

from common import Pipe


class EmailDeduplication(Pipe):
    """Remove duplicated emails from a corpus.

    Recognise duplicates by their metadata.
    Based on logic in select_email(), duplicate will be dropped from rdd.
    """

    def __init__(self, use_metadata=True):
        """Select deduplication method. We might offer more advaced options here."""
        super().__init__()
        self.use_metadata = use_metadata  # IMPLEMENT ME

    def convert_to_tupel(self, document):
        """Convert to tuple RDD where relevant metadata for deduplication are the keys."""
        document_norm = json.loads(document)
        splitting_keys = json.dumps([document_norm['header']['sender']['email'],
                                     document_norm['header']['date'],
                                     document_norm['header']['subject']])

        return (splitting_keys, document)

    def select_email(self, document1, document2):
        """Choose email that remains in corpus."""
        if len(document1) > len(document2):
            return document1
        else:
            return document2

    def convert_from_tupel(self, document_tupel):
        """Convert tupel entry of rdd to usual format for pipeline."""
        return document_tupel[1]

    def run(self, rdd):
        """Run pipe in spark context."""
        return rdd.map(lambda x: self.convert_to_tupel(x)) \
                  .reduceByKey(lambda x, y: self.select_email(x, y)) \
                  .map(lambda x: self.convert_from_tupel(x))


class CorrespondentDeduplication(Pipe):
    """Deduplicate names of correspondants in corpus. M. Müller == Max Müller."""

    def __init__(self):
        """IMPLEMENT ME."""
        raise NotImplementedError
