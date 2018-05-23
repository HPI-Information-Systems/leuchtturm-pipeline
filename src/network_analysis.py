"""This Module makes the network analysis functionality available."""

import py2neo
import networkx as nx
from .community_detection import CommunityDetector
from .role_detection import RoleDetector
from .social_hierarchy_detection import SocialHierarchyDetector
from .common import Pipe


class NetworkAnalyser(Pipe):
    """This class holds the network functionality and gets the needed data.

    Initialize with needed parameters for solr_url, neo4j_host, http_port, bolt_port (for neo4j)
    """

    def __init__(self,
                 conf):
                #  solr_url='http://sopedu.hpi.uni-potsdam.de:8983/solr/emails',
                #  neo4j_host='http://172.16.64.28',  # 'http://sopedu.hpi.uni-potsdam.de',
                #  http_port=61100,
                #  bolt_port=61000):
        """Set solr config and path where rdd is read from."""
        super().__init__(conf)
        self.conf = conf
        self.solr_url = conf.get('solr', 'protocol') + '://' + str(conf.get('solr', 'host')) + ':' + \
            str(conf.get('solr', 'port')) + '/' + conf.get('solr', 'url_path') + '/' + \
            conf.get('solr', 'collection')
        self.neo4j_host = conf.get('neo4j', 'protocol') + '://' + conf.get('neo4j', 'host')
        self.http_port = conf.get('neo4j', 'http_port')
        self.bolt_port = conf.get('neo4j', 'bolt_port')

    def run(self):
        """Run network analysis. Obligatory for Pipe inheritence."""
        self.analyse_network()

    def analyse_network(self):
        """Analyse the network. Parameter upload decides if data in neo4j will be updated."""
        neo_connection = py2neo.Graph(self.neo4j_host, http_port=self.http_port, bolt_port=self.bolt_port)
        edges = neo_connection.run('MATCH (source)-[r]->(target) \
                                    RETURN id(source), id(target), size(r.mail_list) as cnt, r.time_list as tml')
        nodes = neo_connection.run('MATCH (p:Person) RETURN id(p), p.email_addresses, p.identifying_name')

        digraph = nx.DiGraph()

        for edge in edges:
            digraph.add_edge(edge['id(source)'], edge['id(target)'], volume=edge['cnt'], timeline=edge['tml'])

        for node in nodes:
            if not node['p.identifying_name'] == '':
                digraph.add_node(node['id(p)'], email=node['p.email_addresses'])

        graph = digraph.to_undirected()

        print('Number of Nodes: ' + str(graph.number_of_nodes()))
        print('Number of Edges: ' + str(graph.number_of_edges()))

        social_hierarchy_detector = SocialHierarchyDetector()
        social_hierarchy_labels = social_hierarchy_detector.detect_social_hierarchy(digraph)

        community_detector = CommunityDetector(graph)
        community_labels = community_detector.clauset_newman_moore()

        role_detector = RoleDetector()
        role_labels = role_detector.rolx(graph)

        if self.conf.get('neo4j', 'upload'):
            self.update_network(community_labels, "community")
            self.update_network(role_labels, "role")
            self.update_network(social_hierarchy_labels, "hierarchy")

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
