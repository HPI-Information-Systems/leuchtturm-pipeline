"""This Module makes the network analysis functionality available."""

import py2neo
import networkx as nx
from social_hierarchy_detection import SocialHierarchyDetector


class NetworkAnalyser:
    """This class holds the network functionality and gets the needed data.

    Initialize with needed parameters for solr_url, neo4j_host, http_port, bolt_port (for neo4j)
    """

    def __init__(self,
                 solr_url='http://sopedu.hpi.uni-potsdam.de:8983/solr/emails',
                 neo4j_host='http://sopedu.hpi.uni-potsdam.de',
                 http_port=61100,
                 bolt_port=61000):
        """Set solr config and path where rdd is read from."""
        self.solr_url = solr_url
        # querybuilder
        self.neo4j_host = neo4j_host
        self.http_port = http_port
        self.bolt_port = bolt_port

    def analyse_network(self):
        """Analyse the network. Needs no further parameters, will update data in neo4j."""
        print(self.neo4j_host)
        neo_connection = py2neo.Graph(self.neo4j_host, http_port=self.http_port, bolt_port=self.bolt_port)
        edges = neo_connection.run('MATCH (source)-[r]->(target) '
                                   'RETURN id(source), id(target), size(r.mail_list) as cnt')
        # nodes = neo_connection.run('MATCH (p:Person) RETURN id(p), p.name, p.email')

        graph = nx.Graph()
        for edge in edges:
            graph.add_edge(edge['id(source)'], edge['id(target)'])  # , weight=edge['cnt'])

        # for node in nodes:
        #     graph.add_node(node['id(p)'], name='|T|I|M|'.join(node['p.name']), email=node['p.email'])
        social_hierarchy_detector = SocialHierarchyDetector(self.solr_url)
        social_hierarchy_labels = social_hierarchy_detector.detect_social_hierarchy(graph)
        self.update_network(social_hierarchy_labels)
        # nx.write_graphml(graph, "enron.graphml")

    def update_network(self, social_hierarchy_labels):
        """Update neo4j's data with the detected labels."""
        if social_hierarchy_labels:
            print('---------------- finished network analysis ----------------')


na = NetworkAnalyser()
na.analyse_network()
