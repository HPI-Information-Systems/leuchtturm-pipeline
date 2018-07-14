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
        hierarchy_raw = json.load(open('hierarchy.json'))
        community_raw = json.load(open('community.json'))
        role_raw = json.load(open('role.json'))

        hierarchy = dict()
        for data in hierarchy_raw:
            node = data['node_id']
            hierarchy[node] = {}
            for key, value in data.items():
                hierarchy[node][key] = value

        community = sorted(community_raw, key=lambda k: k['node_id'])
        role = sorted(role_raw, key=lambda k: k['node_id'])

        for community_element, role_element in zip(community, role):  # community and role are equally long
            identifying_name = community_element['node_id']
            community_value = community_element['community']
            role_value = role_element['role']
            hierarchy[identifying_name]['community'] = community_value
            hierarchy[identifying_name]['role'] = role_value

        all_in_one = []
        for key, data in hierarchy.items():
            all_in_one.append(data)

        self.update_network(all_in_one)

    def update_network(self, labelled_nodes):
        """Update neo4j's data with the detected labels."""
        if labelled_nodes:
            print(datetime.now(), 'lt_logs', '- start upload of network attributes.', flush=True)
            neo_connection = py2neo.Graph(self.neo4j_host, http_port=self.http_port, bolt_port=self.bolt_port)
            neo_connection.run('UNWIND $labelled_nodes AS ln '
                               'MATCH (node:Person {identifying_name: ln.node_id}) '
                               'SET node.hierarchy = ln.hierarchy '
                               'SET node.community = ln.community '
                               'SET node.role = ln.role '
                               'SET node.number_of_emails = ln.number_of_emails '
                               'SET node.degree = ln.degree '
                               'SET node.clustering_values = ln.clustering_values '
                               'SET node.hubs = ln.hubs '
                               'SET node.authorities = ln.authorities '
                               'SET node.number_of_cliques = ln.number_of_cliques '
                               'SET node.mean_shortest_paths = ln.mean_shortest_paths '
                               'SET node.response_score = ln.response_score '
                               'SET node.average_time = ln.average_time '
                               'SET node.raw_clique_score = ln.raw_clique_score '
                               'SET node.weighted_clique_score = ln.weighted_clique_score',
                               labelled_nodes=labelled_nodes)
            print(datetime.now(), 'lt_logs', '- finished upload of network attributes.', flush=True)
