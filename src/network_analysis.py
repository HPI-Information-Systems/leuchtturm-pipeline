"""This Module makes the network analysis functionality available."""

import py2neo
import networkx as nx
from community_detection import CommunityDetector
from role_detection import RoleDetector


class NetworkAnalyser:
    """This class holds the network functionality and gets the needed data.

    Initialize with needed parameters for solr_url, neo4j_host, http_port, bolt_port (for neo4j)
    """

    def __init__(self,
                 solr_url='http://sopedu.hpi.uni-potsdam.de:8983/solr/emails',
                 neo4j_host='http://172.16.64.28',
                 http_port=61100,
                 bolt_port=61000):
        """Set solr config and path where rdd is read from."""
        self.solr_url = solr_url
        self.neo4j_host = neo4j_host
        self.http_port = http_port
        self.bolt_port = bolt_port

    def analyse_network(self, upload=False):
        """Analyse the network. Parameter upload decides if data in neo4j will be updated."""
        print(self.neo4j_host)
        neo_connection = py2neo.Graph(self.neo4j_host, http_port=self.http_port, bolt_port=self.bolt_port)
        edges = neo_connection.run('MATCH (source)-[r]->(target) '
                                   'RETURN id(source), id(target), size(r.mail_list) as cnt')

        graph = nx.Graph()
        for edge in edges:
            graph.add_edge(edge['id(source)'], edge['id(target)'])  # , weight=edge['cnt'])

        print('Number of Nodes: ' + str(graph.number_of_nodes()))
        print('Number of Edges: ' + str(graph.number_of_edges()))

        community_detector = CommunityDetector()
        community_labels = community_detector.clauset_newman_moore(graph)
        
        role_detector = RoleDetector()
        role_labels = role_detector.rolx(graph)

        if upload:
            self.update_network(community_labels, "community")
            self.update_network(role_labels, "role")

    def update_network(self, labelled_nodes, attribute):
        """Update neo4j's data with the detected labels."""
        if labelled_nodes:
            print('---------------- finished ' + attribute + ' analysis ----------------')

        neo_connection = py2neo.Graph(self.neo4j_host, http_port=self.http_port, bolt_port=self.bolt_port)
        neo_connection.run('UNWIND $labelled_nodes AS ln '
                           'MATCH (node) WHERE ID(node) = ln.node_id '
                           'SET node.' + attribute + ' = ln.' + attribute,
                           labelled_nodes=labelled_nodes, attribute=attribute)
        print('- finished upload of ' + attribute + ' labels.')


na = NetworkAnalyser()
na.analyse_network(True)
