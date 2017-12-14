"""Module containing the Neo4jCommunicator task."""

from datetime import datetime
import json
import luigi
import luigi.contrib.hdfs
from neo4j.v1 import GraphDatabase


DATETIMESTAMP = datetime.now().strftime('%Y-%m-%d_%H-%M')


class Neo4jCommunicator(luigi.Task):
    """This task communicates with neo4j via the official python driver."""

    uri = "bolt://b3986.byod.hpi.de:7687"
    driver = GraphDatabase.driver(uri)
    source_file = luigi.Parameter(default="json.json")

    # def requires(self):
    #     """Require e-mail meta-data."""
    #     return MetadataExtractor()

    # def output(self):
    #     """Write a HDFS target with timestamp."""
    #     return luigi.contrib.hdfs.HdfsTarget('/pipeline/language_detected/' +
    #                                          DATETIMESTAMP +
    #                                          '_language_detected.txt')

    def run(self):
        """Run the neo4j communication."""
        def add_communication(tx, sender, recipients, mail_id):
            """Add relation between sender and recipients. Submethod to prevent 'self' messing up the call.
            
            We iterate over all recipients of a mail and execute the cypher query to neo4j.
            Explanation of the query:
            MERGE: (p:Person {property: value}) --> merge new node with Label Person and properties to database.
            MERGE checks if a node with matching properties exists and either 'merges' oder creates a new node.
            With ON CREATE / ON MATCH we can check if a new node was created or an existing matching one was found.
            The CASEs check if the new value already exists in the property array, then either appends it or not.

            Same procedure for names of sender and recipient and mail_ids of mails between them.
            We use mail-adresses for matching and identifying.
            """
            for recipient in recipients:
                tx.run("MERGE (sender:Person {email: $email_sender}) "
                        "ON CREATE SET sender.name = [$name_sender] "
                        "ON MATCH SET sender.name = "
                            "CASE WHEN NOT $name_sender IN sender.name THEN sender.name + $name_sender "
                                "ELSE sender.name END "
                        "MERGE (recipient:Person {email: $email_recipient}) "
                        "ON CREATE SET recipient.name = [$name_recipient] "
                        "ON MATCH SET recipient.name = "
                            "CASE WHEN NOT $name_recipient IN recipient.name THEN recipient.name + $name_recipient "
                                "ELSE recipient.name END "
                        "MERGE (sender)-[w:WRITESTO]->(recipient) "
                        "ON CREATE SET w.mail_list = [$mail_id] "
                        "ON MATCH SET w.mail_list = "
                            "CASE WHEN NOT $mail_id IN w.mail_list THEN w.mail_list + $mail_id "
                                "ELSE w.mail_list END",
                    name_sender=sender['name'], email_sender=sender['email'], name_recipient=recipient['name'], email_recipient=recipient['email'], mail_id=mail_id)

        with open(self.source_file) as f:
            for line in enumerate(iter(f)):
                mail = json.loads(line)
                if 'sender' in mail['header'].keys():
                    sender = mail['header']['sender']
                if 'recipients' in mail['header'].keys():
                    recipients = mail['header']['recipients']
                if 'doc_id' in mail.keys():
                    mail_id = mail['doc_id']

                with self.driver.session() as session:
                    session.write_transaction(add_communication, sender, recipients, mail_id)
