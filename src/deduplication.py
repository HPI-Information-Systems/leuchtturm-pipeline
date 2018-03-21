"""Deduplication pipes for leuchtturm pipelines."""

import ujson as json


def email_deduplication(rdd):
    """Remove duplicated emails from a corpus.

    Recognise duplicates by their metadata.
    Based on logic in select_email(), duplicate will be dropped from rdd.
    """
    def convert_to_tupel(document):
        """Convert to tuple RDD where relevant metadata for deduplication are the keys."""
        document_norm = json.loads(document)
        splitting_keys = json.dumps([document_norm['header']['sender']['email'],
                                     document_norm['header']['date'],
                                     document_norm['header']['subject']])

        return (splitting_keys, document)

    def select_email(document1, document2):
        """Choose email that remains in corpus."""
        if len(document1) > len(document2):
            return document1
        else:
            return document2

    def convert_from_tupel(document_tupel):
        """Convert tupel entry of rdd to usual format for pipeline."""
        return document_tupel[1]

    return rdd.map(lambda x: convert_to_tupel(x)) \
              .reduceByKey(lambda x, y: select_email(x, y)) \
              .map(lambda x: convert_from_tupel(x))


def correspondent_deduplication(rdd):
    """Deduplicate names of correspondants in corpus. M. Müller == Max Müller."""
    raise NotImplementedError
