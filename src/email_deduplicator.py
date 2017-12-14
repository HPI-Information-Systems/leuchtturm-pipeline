"""Parent class for email deduplicators"""

import json
import luigi
import luigi.contrib.hdfs
from pyspark import SparkContext


class FirstEmailDeduplicator(luigi.Task):
    """Deduplicate emails. Recognize emails by their header."""

    def requires(self):
        """Expect cleaned meta data."""
        raise NotImplementedError('Subclass responsibility')

    def output(self):
        """Write a HDFS target with timestamp."""
        raise NotImplementedError('Subclass responsibility')

    def run(self):
        """Perform duplicate removal."""
        hashmap = set()

        sc = SparkContext()
        data = sc.textFile(self.input().path)
        data = data.collect()

        for idx, val in enumerate(data):
            document = json.loads(val)

            header = document['header']
            sender_mail_address = header['sender']['email']

            recipients_mail_addresses = []
            recipients = header['recipients']
            for recipient in recipients:
                recipients_mail_addresses.append(recipient['email'])

            subject = header['Subject']
            date = header['Date']

            key = [sender_mail_address]
            key.append(recipients_mail_addresses)
            key.append(subject)
            key.append(date)

            if (key in hashmap):
                data.pop(idx)
            else:
                hashmap.add(key)

        with self.output().open('w') as f:
            for date in data:
                f.write(date + '\n')
        sc.stop()
