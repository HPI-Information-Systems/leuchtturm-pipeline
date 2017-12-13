"""Neo4j test sender"""

import luigi
import luigi.contrib.hdfs
from datetime import datetime
import json
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
            for recipient in recipients:
                tx.run("MERGE (sender:Person {name: $name_sender, email: $email_sender}) "
                        "MERGE (recipient:Person {name: $name_recipient, email: $email_recipient}) "
                        "MERGE (sender)-[w:WRITESTO]->(recipient)",
                        # "WITH MATCH (w) WHERE NOT EXISTS(w.mail_list) SET w.mail_list = '' "
                        # "SET w.mail_list = w.mail_list + $mail_id"
                    name_sender=sender['name'], email_sender=sender['email'], name_recipient=recipient['name'], email_recipient=recipient['email'], mail_id=mail_id)
        
        # def print_communication(tx, name):
        #     for record in tx.run("MATCH (sender:Person)-[:WRITESTO]->(recipient) WHERE sender.name = $name "
        #                         "RETURN recipient.name ORDER BY recipient.name", name=name):
        #         print(record["recipient.name"])
            
        with open(self.source_file) as f:
            for index, line in enumerate(iter(f)):
                mail = json.loads(line)
                sender = mail['header']['sender']
                recipients = mail['header']['recipients']
                mail_id = mail['doc_id']
                
                with self.driver.session() as session:
                    session.write_transaction(add_communication, sender, recipients, mail_id)
                    # session.read_transaction(print_communication, sender['name'])
