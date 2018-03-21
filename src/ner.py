"""NER pipes for leuchtturm pipelines."""

import ujson as json
from string import whitespace

import en_core_web_sm as spacy


def spacy_ner(rdd, read_from='text_clean'):
    """NER on texts using spacy.

    Process texts using the en_core_web_sm model.
    Recognize entities of type person, location, organization and assign others to category miscellaneous.
    """
    def run_on_partition(partition):
        """Run task in spark context. Partitionwise for performance reasons."""
        nlp = spacy.load()

        def extract_entities(text):
            """Extract entities from a string."""
            entities = {'person': [],
                        'location': [],
                        'organization': [],
                        'miscellaneous': []}

            # split text into smaller chunks to avoid exceeding memory
            lines = [text[i: i + 1000] for i in range(0, len(text), 1000)]
            for line in lines:
                for entity in filter(lambda x: x.text.strip(whitespace) != '', nlp(line).ents):
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

        def run_on_document(raw_message):
            """Get entities for a leuchtturm document."""
            document = json.loads(raw_message)
            document['entities'] = extract_entities(document[read_from])

            return json.dumps(document)

        for doc in partition:
            yield run_on_document(doc)

    return rdd.mapPartitions(lambda x: run_on_partition(x))
