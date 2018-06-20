"""A module to upload buffered results quickly for debug purposes."""
import json
import py2neo
from datetime import datetime


class NetworkUploader():
    """This class uploads the results of NetworkAnalyser to neo4j."""

    def __init__(self,
                 neo4j_host='http://172.16.64.28',  # 'http://sopedu.hpi.uni-potsdam.de',
                 http_port=61100,
                 bolt_port=61000):
        """Initialize."""
        self.neo4j_host = neo4j_host
        self.http_port = http_port
        self.bolt_port = bolt_port
        self.attribute_names = ['hierarchy', 'community', 'role']

    def run(self):
        """Run network uploader. Obligatory for Pipe inheritence."""
        for attribute_name in self.attribute_names:
            with open(attribute_name + '.json') as f:
                data = json.load(f)
            self.update_network(data, attribute_name)

    def update_network(self, labelled_nodes, attribute):
        """Update neo4j's data with the detected labels."""
        if labelled_nodes:
            print(datetime.now(), 'lt_logs', '- start upload of ' + attribute + ' labels.', flush=True)
            neo_connection = py2neo.Graph(self.neo4j_host, http_port=self.http_port, bolt_port=self.bolt_port)
            neo_connection.run('UNWIND $labelled_nodes AS ln '
                               'MATCH (node) WHERE node.identifying_name = ln.node_id '
                               'SET node.' + attribute + ' = ln.' + attribute,
                               labelled_nodes=labelled_nodes, attribute=attribute)
            print(datetime.now(), 'lt_logs', '- finished upload of ' + attribute + ' labels.', flush=True)


nu = NetworkUploader()
nu.run()
