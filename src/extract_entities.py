"""This module can extract and count entities with spacy inside a Luigi Task."""

import luigi
import requests
from solr_communicator import SolrCommunicator
import spacy
from preprocessing import EMailPreprocesser

nlp = spacy.load('en')


class EntityExtractorAndCounter(luigi.Task):
    """Extract entities with spacy and count them."""

    def requires(self):
        """Require the EMailPreprocessor of the preprocessing module."""
        return EMailPreprocesser()

    def output(self):
        """File the counted entities in a counting_done textfile."""
        return luigi.LocalTarget('counting_done.txt')

    def get_email_body_by_doc_id(self, doc_id):
        """Given a document id, request the email body from solr."""
        connection = requests.get('http://localhost:8983/solr/' +
                                  'email_bodies' +
                                  '/select?q=doc_id:' +
                                  doc_id +
                                  '&wt=json')
        # attention: this doesn't account for the case that multiple matching documents where found!
        return connection.json()['response']['docs'][0]['email_body'][0]

    def make_solr_entity_document(self, doc_id, entity, entity_type, entity_count):
        """Build the json format of a document containing the entity info for passing it to solr."""
        return {
            "doc_id": doc_id,
            "entity": entity,
            "entity_type": entity_type,
            "entity_count": entity_count
        }

    def extract_entities_from_document(self, doc_id):
        """
        Given the document id, extract and count the entities from the email body.

        Also add the extracted entity info as documents to the solr database.
        """
        # EXTRACT ENTITIES
        doc = nlp(self.get_email_body_by_doc_id(doc_id))
        extracted_entities = []
        for entity in doc.ents:
            # some entities have a lot of ugly whitespaces in the beginning/end of string
            stripped_text = entity.text.replace('\n', '\\n').strip()
            duplicate_found = False
            for existing_entity in extracted_entities:
                if existing_entity["entity"] == stripped_text:
                    duplicate_found = True
                    existing_entity["entity_count"] += 1
                    break
            if not duplicate_found:
                extracted_entities.append(
                    self.make_solr_entity_document(doc_id, stripped_text, entity.label_, 1)
                )

        # ADD ENTITIES, THEIR TYPE AND THEIR COUNT 'IN BULK' TO SOLR DATABASE
        extracted_entities_strings = []
        for entity in extracted_entities:
            extracted_entities_strings.append(str(entity).replace("'", '"'))
        print(extracted_entities_strings)
        SolrCommunicator().add_docs('entities', extracted_entities_strings)

    def run(self):
        """Run the extraction in the Luigi Task."""
        for doc_id in SolrCommunicator().request_distinct_doc_ids():
            self.extract_entities_from_document(doc_id)
