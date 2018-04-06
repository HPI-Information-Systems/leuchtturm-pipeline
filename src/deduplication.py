"""Deduplication pipes for leuchtturm pipelines."""

import ujson as json
import hashlib

from .common import Pipe


class EmailDeduplication(Pipe):
    """Remove duplicated emails from a corpus.

    Recognise duplicates by their metadata.
    Based on logic in select_email(), duplicate will be dropped from rdd.
    """

    def __init__(self, is_connected_thread=False):
        """Select deduplication method. We might offer more advaced options here."""
        super().__init__()
        self.is_connected_thread = is_connected_thread

    def convert_to_tupel(self, document):
        """Convert to tuple RDD where relevant metadata for deduplication are the keys."""
        document_norm = json.loads(document)
        splitting_key = ''
        if self.is_connected_thread:
            splitting_key = document_norm['doc_id']
        else:
            splitting_key = json.dumps([document_norm['header']['sender']['email'],
                                        document_norm['header']['date'],
                                        document_norm['header']['subject']])

        return (splitting_key, document)

    def select_email(self, document1, document2):
        """Choose email that remains in corpus."""
        if len(document1) > len(document2):
            return document1
        else:
            return document2

    def convert_from_tupel(self, document_tupel):
        """Convert tupel entry of rdd to usual format for pipeline."""
        return document_tupel[1]

    def generate_doc_id_from_header(self, header):
        """Generate a hash-like id from a header."""
        return hashlib.md5(json.dumps(header).encode()).hexdigest()

    def split_thread(self, raw_message):
        """Split a thread stores in parts into spearate docs."""
        document = json.loads(raw_message)

        parts = []
        for part in document['parts']:
            part['doc_id'] = self.generate_doc_id_from_header(part['header'])
            parts.append(part)

        splitted_emails = []
        for index, part in enumerate(parts):
            part['successor'] = parts[index - 1]['doc_id'] if not index == 0 else None
            part['predecessor'] = parts[index + 1]['doc_id'] if not index == len(parts) - 1 else None
            splitted_emails.append(json.dumps(part))

        return splitted_emails

    def run(self, rdd):
        """Run pipe in spark context."""
        if self.is_connected_thread:
            rdd = rdd.flatMap(lambda x: self.split_thread(x))

        return rdd.map(lambda x: self.convert_to_tupel(x)) \
                  .reduceByKey(lambda x, y: self.select_email(x, y)) \
                  .map(lambda x: self.convert_from_tupel(x))


class CorrespondentDeduplication(Pipe):
    """Deduplicate names of correspondants in corpus. M. Müller == Max Müller."""

    def __init__(self):
        """IMPLEMENT ME."""
        raise NotImplementedError
