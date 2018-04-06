"""Extract signatures from emails."""

import json
import re
import talon
from talon import signature as talon_signature
from .common import Pipe


class SignatureExtraction(Pipe):
    """Extract signatures from emails.

    - uses talon library for main work
    - preprocess emails to improve talon results by removing standard signatures, email attachment notices etc.
    - saves the signature as well as the body without the signature (also doesn't include removal due to preprocessing)
    """

    def __init__(
            self,
            read_from='body',
            write_body_without_signature_to='body_without_signature',
            write_signature_to='signature',
            write_sent_from_mobile_to='sent_from_mobile'
    ):
        """Set params."""
        super().__init__()
        self.read_from = read_from
        self.write_body_without_signature_to = write_body_without_signature_to
        self.write_signature_to = write_signature_to
        self.write_sent_from_mobile_to = write_sent_from_mobile_to

    def _get_mobile_signature_patterns(self):
        return [
            r'--------------------------\r?\nSent from my BlackBerry Wireless Handheld \(www\.BlackBerry\.net\)\r?\s*$'
        ]

    def _get_standard_signatures(self):
        return [
            (
                r'_________________________________________________________________\r?\n'
                r'Join the world\'s largest e-mail service with MSN Hotmail. \r?\n'
                r'http:\/\/www\.hotmail\.com'
            ),
            (
                r'_{50,60}\r?\n'
                r'Do You Yahoo!\?\r?\n'
                r'(.*\r?\n)?'
                r'.*yahoo\.com.*\r?\s*'
            ),
            (
                '\*{78}\r?\n'
                '*\*{7}\r?\n\n'
                'This message is a PRIVATE communication\.   If you are not the intended\r?\n'
                'recipient, please do not read, copy, or use it, and do not disclose it to\r?\n'
                'others\.  Please notify the sender of the delivery error by replying to this\r?\n'
                'message, and then delete it from your system\.  Thank you\.\r?\n'
                '\*{78}\r?\n'
                '\*{7}\r?\n*'
                'For more information on McDERMOTT, WILL & EMERY please visit our website at:\r?\n'
                'http:\/\/www\.mwe\.com\/\s*'
            ),
            (
                '\*{7}\r?\n*'
                'For more information on McDERMOTT, WILL & EMERY please visit our website at:\r?\n'
                'http:\/\/www\.mwe\.com\/\s*'
            )
        ]

    def remove_standard_signatures(self, body):
        """Remove standard signatures from an email body.

        - generic signatures from email providers
        - mobile signatures (this information might be useful later for classification of emails)
        """
        body = re.sub('\s*$', '', body)

        for standard_signature in self._get_standard_signatures():
            body = re.sub(standard_signature, '\n', body)

        sent_from_mobile = None
        for mobile_signature in self._get_mobile_signature_patterns():
            if re.search(mobile_signature, body):
                sent_from_mobile = True
                body = re.sub(mobile_signature, '\n', body)

        return body, sent_from_mobile

    def remove_attachment_notices(self, body):
        """Remove strings that hint at attached files.

        These strings often occur just before, in the middle of, or right after the signature and irritate Talon
        """
        file_formats_pattern = r'(\w{2,4})'
        attached_files_patterns = [r'(\(See attached\s{,3}file: (.+)\.' + file_formats_pattern + '?\)\s*)+',
                                   r'(<<(.+)\.' + file_formats_pattern + r'?\s*>>\s*(=20)?\s*)+',
                                   r'(\n\s?-\s?.+\.' + file_formats_pattern + r'(=20)?\s*)+$',
                                   r'(\n\[image\](=20)?\s*)+$']

        for pattern in attached_files_patterns:
            body = re.sub(pattern, '\n', body, flags=re.IGNORECASE)

        return body

    def extract_signature(self, body, sender_email_address):
        """Apply talon to the preprocessed body.

        Uses the email address of the sending correspondent to improve extraction results.
        """
        body, signature = talon_signature.extract(
            body,
            sender=sender_email_address
        )
        if not signature:
            signature = ''
        return body, signature

    def run_on_partition(self, data_items):
        """Apply pure extraction task partition-wise so that talon models don't have to be reloaded for each email."""
        talon.init()
        for data_item in data_items:
            document = json.loads(data_item)
            document[self.write_body_without_signature_to], document[self.write_signature_to] = \
                self.extract_signature(
                    document[self.write_body_without_signature_to],
                    document['header']['sender']['email']
            )
            yield json.dumps(document)

    def run_on_document(self, data_item):
        """Apply signature-specific preprocessing tasks to a leuchtturm document."""
        document = json.loads(data_item)

        # factor these out of here
        document[self.write_body_without_signature_to] = document[self.read_from]
        document[self.write_body_without_signature_to] = \
            self.remove_attachment_notices(document[self.write_body_without_signature_to])
        document[self.write_body_without_signature_to], document[self.write_sent_from_mobile_to] = \
            self.remove_standard_signatures(document[self.write_body_without_signature_to])

        return json.dumps(document)

    def run(self, rdd):
        """Run pipe in spark context."""
        return rdd.map(self.run_on_document) \
            .mapPartitions(self.run_on_partition)
