"""NER pipes for leuchtturm pipelines."""

import ujson as json
from string import whitespace

import en_core_web_sm as spacy

from .common import Pipe


class SpacyNer(Pipe):
    """NER on texts using spacy.

    Process texts using the en_core_web_sm model.
    Recognize entities of type person, location, organization and assign others to category miscellaneous.
    """

    def __init__(self, read_from='text_clean'):
        """Set params. read_from: field to search entities in."""
        super().__init__()
        self.read_from = read_from

    def extract_entities(self, text, spacy_model):
        """Extract entities from a string."""
        entities = {'person': [],
                    'location': [],
                    'organization': [],
                    'miscellaneous': []}

        # split text into smaller chunks to avoid exceeding memory
        lines = [text[i: i + 1000] for i in range(0, len(text), 1000)]
        for line in lines:
            for entity in filter(lambda x: x.text.strip(whitespace) != '', spacy_model(line).ents):
                if (entity.label_ == 'PERSON'):
                    entities['person'].append(entity.text.strip(whitespace))
                elif (entity.label_ == 'LOC' or entity.label_ == 'GPE' or entity.label_ == 'FAC'):
                    entities['location'].append(entity.text.strip(whitespace))
                elif (entity.label_ == 'ORG' or entity.label_ == 'NORP'):
                    entities['organization'].append(entity.text.strip(whitespace))
                elif (entity.label_ == 'PRODUCT' or entity.label_ == 'EVENT' or entity.label_ == 'WORK_OF_ART'):
                    entities['miscellaneous'].append(entity.text.strip(whitespace))

        # deduplicate entities in their categories
        return {'person': list(set(entities['person'])),
                'location': list(set(entities['location'])),
                'organization': list(set(entities['organization'])),
                'miscellaneous': list(set(entities['miscellaneous']))}

    def load_spacy(self):
        """Load spacy model."""
        return spacy.load()

    def run_on_document(self, raw_message, spacy_model=None):
        """Get entities for a leuchtturm document."""
        spacy_model = spacy_model if spacy_model is not None else self.load_spacy()
        document = json.loads(raw_message)
        document['entities'] = self.extract_entities(document[self.read_from], spacy_model)

        return json.dumps(document)

    def run_on_partition(self, partition):
        """Run task in spark context. Partitionwise for performance reasosn."""
        spacy_model = self.load_spacy()

        for doc in partition:
            yield self.run_on_document(doc, spacy_model=spacy_model)

    def run(self, rdd):
        """Run task in a spark context."""
        return rdd.mapPartitions(lambda x: self.run_on_partition(x))
