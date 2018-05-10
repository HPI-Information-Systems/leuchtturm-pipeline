"""Module concerned with the extraction and aggregation of information about correspondents.

Information is mostly drawn from extracted signatures.
"Aggregation" means collecting all information about a correspondent.
"""

import ujson as json
import re
import regex
from datetime import datetime
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
    phone_types = {
        'phone_numbers_office': r'(off|ph|tel|dir|voice)',  # this will be default
        'phone_numbers_cell': r'(cell|mobile|mob)',
        'phone_numbers_fax': r'(fax|fx|facs|facsimile|facsim)',
        'phone_numbers_home': r'home'
    }.items()

    def _get_phone_number_type(self, enclosing_line):
        for pattern_type, pattern in self.phone_types:
            if re.search(pattern, enclosing_line, flags=re.IGNORECASE):
                return pattern_type
        return list(self.phone_types)[0][0]  # set type to 'office' by default

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
        phone_numbers = {key: [] for key in [tuple[0] for tuple in self.phone_types]}

        for line in signature.split('\n'):
            phone_number_match = re.search(self.phone_pattern, line, flags=re.IGNORECASE)
            if phone_number_match:
                phone_number_type = self._get_phone_number_type(line)
                phone_numbers[phone_number_type].append(phone_number_match.group(1))
        return phone_numbers

    def extract_email_addresses_from(self, signature):
        """Extract email addresses that possibly occur in the signature."""
        email_address_pattern = r'\b[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+\b'
        return re.findall(email_address_pattern, signature)

    def extract_aliases_from(self, signature, email_address):
        """Extract aliases of the correspondent's name from signature that are similar to their email address."""
        email_username = email_address.split('@')[0]
        if len(email_username) < 3:
            return []

        email_username_start = regex.escape(email_username[:3])
        email_username_end = regex.escape(email_username[-3:])
        alias_prefix_pattern = r'(?:^|\n)\b(' + email_username_start + r'[\w -.]*)\s?(?:\n|$)'
        alias_postfix_pattern = r'(?:^|\n)([\w -.]*' + email_username_end + r')(?:$|\n)'
        first_two_signature_lines = '\n'.join([line for line in signature.split('\n') if line != ''][:2])

        aliases = set()
        try:
            # note: using regex module (not re) for overlapping matches (necessary because of shared \n between aliases)
            aliases = set(
                regex.findall(alias_prefix_pattern, signature, overlapped=True, flags=re.IGNORECASE)
            )
            aliases.update(set(
                regex.findall(alias_postfix_pattern, first_two_signature_lines, overlapped=True, flags=re.IGNORECASE)
            ))
        except Exception:
            print('lt_logs', datetime.now(),
                  'Error in Alias Extraction',
                  'aliases so far', aliases,
                  'prefix pattern', alias_prefix_pattern,
                  'signature', signature.replace('\n', '\\n'),
                  'postfix pattern', alias_postfix_pattern,
                  'first_two_signature_lines', first_two_signature_lines.replace('\n', '\\n'),
                  flush=True)

        aliases = [alias.strip() for alias in list(aliases)]
        return aliases

    def extract_writes_to_relationship(self, recipients):
        """Extract the email addresses of correspondents that the correspondent at hand writes emails to."""
        return list(set([recipient['email'] for recipient in recipients]))

    def convert_and_rename_fields(self, document):
        """Convert all fields to list types for easier two-phase merging, rename fields to correspondent-semantic."""
        document['email_addresses'] = [e for e in [document.pop('sender_email_address')] if e]
        document['identifying_names'] = [e for e in [document.pop('sender_name')] if e]
        document['aliases_from_signature'] = document.pop('sender_aliases')
        document['signatures'] = [document.pop('signature')]
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
                'phone_numbers_office': [],
                'phone_numbers_cell': [],
                'phone_numbers_fax': [],
                'phone_numbers_home': [],
                'email_addresses_from_signature': [],
                'writes_to': [],
                'recipients': []
            }

        correspondents = []
        for recipient in document['recipients']:
            correspondents.append(build_correspondent(recipient['name'], recipient['email']))
        return correspondents

    def run_on_document(self, data_item):
        """Apply correspondent data extraction to a leuchtturm document. Return list of leuchtturm documents."""
        document = json.loads(data_item)

        document = self.filter_document_keys(document)
        phone_numbers = self.extract_phone_numbers_from(document['signature'])
        document.update(phone_numbers)
        document['email_addresses_from_signature'] = self.extract_email_addresses_from(document['signature'])
        document['sender_aliases'] = self.extract_aliases_from(
            document['signature'],
            document['sender_email_address']
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

    def __init__(self):
        """Set params."""
        super().__init__()

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
            unified_person['identifying_names'] = [max(unified_person['aliases_from_signature'])]

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


class CorrespondentIdInjection(Pipe):
    """Write the identifying_name back onto sender/recipient entries of each email to enable proper linking in FE."""

    def __init__(self, correspondent_rdd):
        """Set up class attributes."""
        super().__init__()
        self.correspondent_rdd = [json.loads(corr) for corr in correspondent_rdd.value]

    def _find_matching_correspondent(self, original_name, original_email_address):
        """Look up identifying_name of correspondent object inside broadcast.

        First by email_addresses, then by identifying_names and then by aliases (in that order).
        """
        correspondent = None
        if original_email_address:
            correspondent = next(
                (corr for corr in self.correspondent_rdd if original_email_address in corr['email_addresses']), None)
        if not correspondent:
            correspondent = next(
                (corr for corr in self.correspondent_rdd if original_name in corr['identifying_names']), None)
        if not correspondent:
            correspondent = next(
                (corr for corr in self.correspondent_rdd if original_name in corr['aliases']), None)
        return correspondent

    def assign_identifying_name_for_sender(self, document):
        """Write identifying_name back onto sender of one email."""
        original_name = document['header']['sender']['name']
        original_email_address = document['header']['sender']['email']

        correspondent = self._find_matching_correspondent(original_name, original_email_address)

        if correspondent and correspondent['identifying_names']:
            document['header']['sender']['identifying_name'] = correspondent['identifying_names'][0]
        else:
            document['header']['sender']['identifying_name'] = ''

        return document

    def assign_identifying_name_for_recipients(self, document):
        """Write identifying_name back onto recipients of one email."""
        for i in range(len(document['header']['recipients'])):
            original_name = document['header']['recipients'][i]['name']
            original_email_address = document['header']['recipients'][i]['email']

            correspondent = self._find_matching_correspondent(original_name, original_email_address)

            if correspondent and correspondent['identifying_names']:
                document['header']['recipients'][i]['identifying_name'] = correspondent['identifying_names'][0]
            else:
                document['header']['recipients'][i]['identifying_name'] = ''
        return document

    def run_on_document(self, data):
        """Wrap writing the identifying_name back onto sender and recipient entries for one email."""
        document = json.loads(data)
        document = self.assign_identifying_name_for_sender(document)
        document = self.assign_identifying_name_for_recipients(document)
        return json.dumps(document)

    def run(self, rdd):
        """Run on RDD."""
        return rdd.map(self.run_on_document)
