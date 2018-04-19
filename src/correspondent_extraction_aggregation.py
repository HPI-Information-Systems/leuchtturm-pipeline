"""Module concerned with the extraction and aggregation of information about correspondents.

Information is mostly drawn from extracted signatures.
"Aggregation" means collecting all information about a correspondent.
"""

import ujson as json
import re
import regex
from .common import Pipe


class CorrespondentDataExtraction(Pipe):
    """Extract single pieces of information about correspondents from emails.

    - start by removing all keys except for signature, header->sender->email and header->recipients
    - from signature: phone numbers, email address, aliases
    - writes_to relationship
    """

    def __init__(self):
        """Set constant regex patterns for class access."""
        super().__init__()

    phone_pattern = r'(\(?\b[0-9]{3}\)?(?:-|\.|/| {1,2}| - )?[0-9]{3}(?:-|\.|/| {1,2}| - )?[0-9]{4,5}\b)'
    phone_type_patterns = [r'(off|ph|tel|dir|voice)',
                           r'(cell|mobile|mob)',
                           r'(fax|fx|facs|facsimile|facsim)',
                           r'home']
    phone_type_keys = ['phone_numbers_office',  # this will be default
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
        """Remove key-values that are irrelevant to correspondent information extraction."""
        return {
            'signature': document['signature'],
            'sender_email_address': document['header']['sender']['email'],
            'sender_name': document['header']['sender']['name'],
            'recipients': document['header']['recipients']
        }

    def extract_phone_numbers_from(self, signature):
        """Extract phone numbers and their type (office, cell, home, fax) from a signature."""
        phone_numbers = dict()
        for key in self.phone_type_keys:
            phone_numbers[key] = []
        split_signature = self._split_on_phone_numbers(signature)

        # iterate over all phone numbers found in the signature
        # i is pointing to the current phone number string, i-1 and i+1 to strings before and after the phone number
        for i in range(1, len(split_signature), 2):
            # get the line on which the phone number occurs (without the phone number itself)
            # '...\nFax: 123-4567-8910 ab\n...' => 'Fax:  ab'
            enclosing_line = split_signature[i - 1].rpartition('\n')[-1] + \
                split_signature[i + 1].partition('\n')[0]
            type = self._get_phone_number_type(enclosing_line)
            phone_numbers[type].append(split_signature[i])
        return phone_numbers

    def extract_email_addresses_from(self, signature):
        """Extract email addresses that possibly occur in the signature."""
        email_address_pattern = r'\b[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+\b'
        return re.findall(email_address_pattern, signature)

    def extract_aliases_from(self, signature, first_email_address_characters):
        """Extract aliases of the correspondent's name from signature that are similar to their email address."""
        if not first_email_address_characters:
            return []
        alias_name_pattern = r'(?:^|\n)\b(' + first_email_address_characters + r'[\w -.]*)\s?(?:\n|$)'
        # note: using regex module (not re) so that matches can overlap (necessary because of shared \n between aliases)
        return regex.findall(alias_name_pattern, signature, overlapped=True, flags=re.IGNORECASE)

    def extract_writes_to_relationship(self, recipients):
        """Extract the email addresses of correspondents that the correspondent at hand writes emails to."""
        return list(set([recipient['email'] for recipient in recipients]))

    def convert_and_rename_fields(self, document):
        """Convert all fields to list types for easier two-phase merging, rename fields to correspondent-semantic."""
        document['email_addresses'] = [document['sender_email_address']] if document['sender_email_address'] else []
        document['identifying_names'] = [document['sender_name']] if document['sender_name'] else []
        document['aliases_from_signature'] = document['sender_aliases']
        document['signatures'] = [document['signature']]
        for key in ['sender_email_address', 'sender_name', 'sender_aliases', 'signature']:
            del document[key]
        document['aliases'] = []
        return document

    def extract_receiving_correspondents(self, document):
        """Set up correspondents for receiving correspondents from emails. Sending correspondents already exist."""
        def build_correspondent(recipient_name, recipient_email_address):
            return {
                'signatures': [],
                'email_addresses': [recipient_email_address] if recipient_email_address else [],
                'identifying_names': [recipient_name] if recipient_name else [],
                'aliases_from_signature': [],
                'aliases': [],
                "phone_numbers_office": [],
                "phone_numbers_cell": [],
                "phone_numbers_fax": [],
                "phone_numbers_home": [],
                "email_addresses_from_signature": [],
                "writes_to": [],
                'recipients': []
            }

        documents = []
        for recipient in document['recipients']:
            documents.append(build_correspondent(recipient['name'], recipient['email']))
        return documents

    def run_on_document(self, data_item):
        """Apply correspondent data extraction to a leuchtturm document. Return list of leuchtturm documents."""
        document = json.loads(data_item)

        document = self.filter_document_keys(document)
        phone_numbers = self.extract_phone_numbers_from(document['signature'])
        document.update(phone_numbers)
        document['email_addresses_from_signature'] = self.extract_email_addresses_from(document['signature'])
        first_email_address_characters = document['sender_email_address'][:3]
        document['sender_aliases'] = self.extract_aliases_from(
            document['signature'],
            first_email_address_characters
        )
        document['writes_to'] = self.extract_writes_to_relationship(document['recipients'])
        document = self.convert_and_rename_fields(document)
        documents = [document] + self.extract_receiving_correspondents(document)

        return [json.dumps(document) for document in documents]

    def run(self, rdd):
        """Run pipe in spark context."""
        return rdd.flatMap(self.run_on_document)


class CorrespondentDataAggregation(Pipe):
    """Aggregate all the correspondent information found in different email to a single correspondent object.

    - uses the sender name extracted from email header to identify a correspondent
    - input correspondent objects are first stripped of irrelevant keys and non-list-type fields (except for identifying
      name field) are transformed to list-type fields
    - this allows for simple merging of correspondent objects into a single object afterwards
    """

    def prepare_for_reduction(self, data):
        """Remove irrelevant key-values, make all fields lists except for identifying name."""
        document = json.loads(data)
        document['source_count'] = 1
        irrelevant_keys = ['recipients']
        for key in irrelevant_keys:
            del document[key]

        return json.dumps(document)

    def extract_data_from_tuple(self, data):
        """Transform the generated tuple back into leuchtturm document."""
        return data[1]

    def convert_to_email_address_tuple(self, data):
        """Convert to tuple as preparation for reduceByKey to work right."""
        document = json.loads(data)
        # we know that there can only be one element in document['email_addresses']
        splitting_keys = json.dumps(document['email_addresses'])
        return splitting_keys, data

    def merge_correspondents_by_email_address(self, data1, data2):
        """Merge all information about a correspondent from two different objects, avoid duplicates."""
        correspondent1 = json.loads(data1)
        correspondent2 = json.loads(data2)

        unified_person = {
            'email_addresses': correspondent1['email_addresses'],
            'source_count': correspondent1['source_count'] + correspondent2['source_count'],
        }

        for key in correspondent1:
            if key not in ['email_addresses', 'source_count']:
                unified_person[key] = list(set(correspondent1[key] + correspondent2[key]))

        if not unified_person['identifying_names'] and unified_person['aliases_from_signature']:
            unified_person['identifying_names'] = max(unified_person['aliases_from_signature'])

        if len(unified_person['identifying_names']) > 1:
            identifying_name = max(unified_person['identifying_names'])
            unified_person['aliases'] = unified_person['identifying_names']
            unified_person['aliases'].remove(identifying_name)
            unified_person['identifying_names'] = [identifying_name]

        return json.dumps(unified_person)

    def convert_to_name_tuple(self, data):
        """Convert to tuple as preparation for reduceByKey to work right."""
        document = json.loads(data)
        splitting_keys = json.dumps(document['identifying_names'])
        return splitting_keys, data

    def merge_correspondents_by_name(self, data1, data2):
        """Merge all information about a correspondent from two different objects, avoid duplicates."""
        correspondent1 = json.loads(data1)
        correspondent2 = json.loads(data2)

        unified_person = {
            'identifying_names': correspondent1['identifying_names'],
            'source_count': correspondent1['source_count'] + correspondent2['source_count'],
        }

        for key in correspondent1:
            if key not in ['identifying_names', 'source_count']:
                unified_person[key] = list(set(correspondent1[key] + correspondent2[key]))
        return json.dumps(unified_person)

    def use_email_addresses_as_identifying_names(self, data):
        """In case identifying_names is still empty at this point, use the email_addresses property as a backup."""
        document = json.loads(data)
        document['identifying_names'] = document['email_addresses']
        return json.dumps(document)

    def run(self, rdd):
        """Run pipe in spark context."""
        rdd = rdd.map(self.prepare_for_reduction)

        rdd_with_email_addresses = rdd.filter(lambda data: json.loads(data)['email_addresses']) \
                                      .map(self.convert_to_email_address_tuple) \
                                      .reduceByKey(self.merge_correspondents_by_email_address) \
                                      .map(self.extract_data_from_tuple)
        rdd_without_email_addresses = rdd.filter(lambda data: not json.loads(data)['email_addresses'])
        rdd = rdd_with_email_addresses.union(rdd_without_email_addresses)

        rdd_with_identifying_names = rdd.filter(lambda data: json.loads(data)['identifying_names']) \
                                        .map(self.convert_to_name_tuple) \
                                        .reduceByKey(self.merge_correspondents_by_name) \
                                        .map(self.extract_data_from_tuple)
        rdd_without_identifying_names = rdd.filter(lambda data: not json.loads(data)['identifying_names']) \
                                           .map(self.use_email_addresses_as_identifying_names)
        rdd = rdd_with_identifying_names.union(rdd_without_identifying_names)

        return rdd
