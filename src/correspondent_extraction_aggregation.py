import json
import re
import regex
from .common import Pipe


class CorrespondentDataExtraction(Pipe):
    # TODO: update this
    """Split emails at their inline headers.

    Maximize information by adding inline coversations as separate documents.
    Use of this pipe is discouraged since correspondent deduplication is not yet implemented.
    """
    """Extract single pieces of information about correspondents from emails."""

    # TODO: remove this enterely?
    def __init__(self):
        """Set params."""
        super().__init__()
        self.phone_pattern = r'(\(?\b[0-9]{3}\)?(?:-|\.|/| {1,2}| - )?[0-9]{3}(?:-|\.|/| {1,2}| - )?[0-9]{4,5}\b)'
        self.phone_type_patterns = [r'(off|ph|tel|dir|voice)',
                                    r'(cell|mobile|mob)',
                                    r'(fax|fx|facs|facsimile|facsim)',
                                    r'home']
        self.phone_type_keys = ['phone_numbers_office',  # this will be default
                                'phone_numbers_cell',
                                'phone_numbers_fax',
                                'phone_numbers_home']

    def _split_on_phone_numbers(self, signature):
        return re.split(self.phone_pattern, signature, flags=re.IGNORECASE)

    def _get_phone_number_type(self, enclosing_line):
        for i, pattern in enumerate(self.phone_type_patterns):
            if re.search(pattern, enclosing_line, flags=re.IGNORECASE):
                return self.phone_type_keys[i]
        return self.phone_type_keys[0]  # set type to 'office' by default

    def filter_document_keys(self, document):
        return {
            'signature': document['signature'],
            'sender_email_address': document['header']['sender']['email'],
            'recipients': document['header']['recipients']
        }

    def extract_phone_numbers_from(self, signature):
        phone_numbers = dict()
        for key in self.phone_type_keys:
            phone_numbers[key] = []
        split_signature = self._split_on_phone_numbers(signature)

        # iterate over all phone numbers found in the signature
        # i is pointing to the current phone number string, i-1 and i+1 to strings before and after the phone number
        for i in range(1, len(split_signature), 2):
            # get the line on which the phone number occurs (without the phone number itself)
            # '...\nFax: 123-4567-8910 ab\n...' => 'Fax:  ab'
            enclosing_line = split_signature[i - 1].rpartition('\n')[-1] \
                             + split_signature[i + 1].partition('\n')[0]
            type = self._get_phone_number_type(enclosing_line)
            phone_numbers[type].append(split_signature[i])
        return phone_numbers

    def extract_email_addresses_from(self, signature):
        email_address_pattern = r'\b[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+\b'
        return re.findall(email_address_pattern, signature)

    def extract_aliases_from(self, signature, first_email_address_characters):
        alias_name_pattern = r'(?:^|\n)\b(' + first_email_address_characters + r'[\w -.]*)\s?(?:\n|$)'
        # note: using regex module (not re) so that matches can overlap (necessary because of shared \n between aliases)
        return regex.findall(alias_name_pattern, signature, overlapped=True, flags=re.IGNORECASE)

    def extract_writes_to_relationship(self, recipients):
        return list(set([recipient['email'] for recipient in recipients]))

    def run_on_document(self, data_item):
        # TODO: update comment
        """Apply email splitting to a leuchtturm document. Return list of leuchtturm documents."""
        document = json.loads(data_item)

        document = self.filter_document_keys(document)
        phone_numbers = self.extract_phone_numbers_from(document['signature'])
        document.update(phone_numbers)
        document['email_addresses_from_signature'] = self.extract_email_addresses_from(document['signature'])
        # first_email_address_characters =
        document['sender_aliases'] = self.extract_aliases_from(document['signature'], document['sender_email_address'][:3])
        document['writes_to'] = self.extract_writes_to_relationship(document['recipients'])

        return json.dumps(document)


    def run(self, rdd):
        """Run pipe in spark context."""
        return rdd.map(self.run_on_document)


class CorrespondentDataAggregation(Pipe):
    # TODO: update this
    """Split emails at their inline headers.

    Maximize information by adding inline coversations as separate documents.
    Use of this pipe is discouraged since correspondent deduplication is not yet implemented.
    """
    """Extract single pieces of information about correspondents from emails."""

    # TODO: remove this enterely?
    def __init__(self):
        """Set params."""
        super().__init__()

    def _remove_irrelevant_key_values(self, document):
        irrelevant_keys = ['recipients']
        for key in irrelevant_keys:
            del document[key]
        return document

    def _convert_fields_to_list_type(self, document):
        document['signatures'] = [document['signature']]
        del document['signature']
        return document

    def prepare_for_reduction(self, data):
        document = json.loads(data)
        document = self._remove_irrelevant_key_values(document)
        document = self._convert_fields_to_list_type(document)
        return json.dumps(document)

    def convert_to_tuple(self, data):
        document = json.loads(data)
        splitting_keys = json.dumps(document['sender_email_address'])
        return splitting_keys, data

    def merge_correspondents(self, data1, data2):
        correspondent1 = json.loads(data1)
        correspondent2 = json.loads(data2)

        unified_person = {
            'sender_email_address': correspondent1['sender_email_address'],
        }

        for key in correspondent1:
            if key != 'sender_email_address':
                unified_person[key] = list(set(correspondent1[key] + correspondent2[key]))
        return json.dumps(unified_person)

    def revert_to_json(self, data):
        return data[1]

    def rename_keys(self, data):
        document = json.loads(data)
        document['email_address'] = document['sender_email_address']
        del document['sender_email_address']
        document['aliases'] = document['sender_aliases']
        del document['sender_aliases']
        return json.dumps(document)

    def run(self, rdd):
        """Run pipe in spark context."""
        return rdd.map(self.prepare_for_reduction) \
                  .map(self.convert_to_tuple) \
                  .reduceByKey(self.merge_correspondents) \
                  .map(self.revert_to_json) \
                  .map(self.rename_keys)
