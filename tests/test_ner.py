"""Test ner module."""

import json

from src.ner import SpacyNer
from config.config import Config


def test_entity_extraction():
    """Test some examples."""
    conf = Config(['-c', 'config/testconfig.ini'])
    task = SpacyNer(conf, read_from='text')
    task.load_spacy()

    doc = json.dumps({'text': 'London is the capital of the United Kingdom.'})
    ents = task.run_on_document(doc)

    assert 'entities' in json.loads(ents)
    assert 'London', 'United Kingdom' in json.loads(ents)['entities']
