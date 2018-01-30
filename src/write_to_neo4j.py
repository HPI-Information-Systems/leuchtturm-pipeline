"""This module writes pipeline results to a neo4j database."""

from settings import hdfs_client_url, neo4j_client_url, path_pipeline_results_short
import json
from neo4j.v1 import DirectDriver
from hdfs import Client


def write_to_neo4j():
    """Write communication data from pipeline to a predefined neo4j database.

    Requires: Text mining pipline ran.
    Arguments: none.
    Returns: void.
    """
    hdfs_client = Client(hdfs_client_url)
    driver = DirectDriver(neo4j_client_url)

    with driver.session() as session:
        for partition in hdfs_client.list(path_pipeline_results_short):
            with hdfs_client.read(path_pipeline_results_short + '/' + partition,
                                  encoding='utf-8',
                                  delimiter='\n') as reader:
                for document in reader:
                    if (len(document) != 0):
                        sender = {"name": "", "email": ""}
                        recipients = []
                        mail_id = ""
                        mail = json.loads(document)
                        if 'sender' in mail['header'].keys():
                            sender = mail['header']['sender']
                        if 'recipients' in mail['header'].keys():
                            recipients = mail['header']['recipients']
                        if 'doc_id' in mail.keys():
                            mail_id = mail['doc_id']

                        for recipient in recipients:
                            session.run("MERGE (sender:Person {email: $email_sender}) "
                                        "ON CREATE SET sender.name = [$name_sender] "
                                        "ON MATCH SET sender.name = "
                                        "CASE WHEN NOT $name_sender IN sender.name "
                                        "THEN sender.name + $name_sender "
                                        "ELSE sender.name END "
                                        "MERGE (recipient:Person {email: $email_recipient}) "
                                        "ON CREATE SET recipient.name = [$name_recipient] "
                                        "ON MATCH SET recipient.name = "
                                        "CASE WHEN NOT $name_recipient IN recipient.name "
                                        "THEN recipient.name + $name_recipient "
                                        "ELSE recipient.name END "
                                        "MERGE (sender)-[w:WRITESTO]->(recipient) "
                                        "ON CREATE SET w.mail_list = [$mail_id] "
                                        "ON MATCH SET w.mail_list = "
                                        "CASE WHEN NOT $mail_id IN w.mail_list "
                                        "THEN w.mail_list + $mail_id "
                                        "ELSE w.mail_list END",
                                        name_sender=sender['name'],
                                        email_sender=sender['email'],
                                        name_recipient=recipient['name'],
                                        email_recipient=recipient['email'],
                                        mail_id=mail_id)


if __name__ == '__main__':
    write_to_neo4j()
