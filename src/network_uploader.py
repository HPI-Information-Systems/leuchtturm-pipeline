"""A module to upload buffered results quickly for debug purposes."""
import json
import py2neo
from datetime import datetime
from .common import Pipe


class NetworkUploader(Pipe):
    """This class uploads the results of NetworkAnalyser to neo4j."""

    def __init__(self, conf):
        """Initialize with a config."""
        super().__init__(conf)
        self.neo4j_host = conf.get('neo4j', 'protocol') + '://' + conf.get('neo4j', 'host')
        self.http_port = conf.get('neo4j', 'http_port')
        self.bolt_port = conf.get('neo4j', 'bolt_port')
        self.attribute_names = ['hierarchy', 'community', 'role']
        self.metrics = ['number_of_emails', 'degree', 'clustering_values', 'hubs', 'authorities', 'number_of_cliques',
                        'mean_shortest_paths', 'response_score', 'average_time', 'raw_clique_score',
                        'weighted_clique_score']

    def run(self):
        """Run network uploader. Obligatory for Pipe inheritence."""
        for attribute_name in self.attribute_names:
            with open(attribute_name + '.json') as f:
                data = json.load(f)
            self.update_network(data, attribute_name)
            if attribute_name == 'hierarchy':
                for metric in self.metrics:
                    self.update_network(data, metric)

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
