import py2neo
from community_detection import CommunityDetector
from social_hierarchy_detection import SocialHierarchyDetector
import networkx as nx


class NetworkAnalyser:
    def __init__(self,
                 solr_url='http://sopedu.hpi.uni-potsdam.de:8983/solr/emails',
                 neo4j_host='http://sopedu.hpi.uni-potsdam.de',
                 http_port=60100,
                 bolt_port=60000):
        """Set solr config and path where rdd is read from."""
        self.solr_url = solr_url
        # querybuilder
        self.neo4j_host = neo4j_host
        self.http_port = http_port
        self.bolt_port = bolt_port

    def analyse_network(self):
        print(self.neo4j_host)
        neo_connection = py2neo.Graph(self.neo4j_host, http_port=self.http_port, bolt_port=self.bolt_port)
        edges = neo_connection.run('MATCH (source)-[r]->(target) RETURN id(source), id(target), size(r.mail_list) as cnt')
        #nodes = neo_connection.run('MATCH (p:Person) RETURN id(p), p.name, p.email')

        graph = nx.Graph()
        for edge in edges:
            graph.add_edge(edge['id(source)'], edge['id(target)']) # , weight=edge['cnt'])

        # for node in nodes:
        #     graph.add_node(node['id(p)'], name='|T|I|M|'.join(node['p.name']), email=node['p.email'])
        #  community_detector = CommunityDetector()
        # graph = community_detector.detect_communities(graph)
        social_hierarchy_detector = SocialHierarchyDetector(self.solr_url)
        graph = social_hierarchy_detector.detect_social_hierarchy(graph)
        self.upload_network(graph)
        # nx.write_graphml(graph, "enron.graphml")

    def upload_network(self, graph):
        print(graph.number_of_nodes())
        print(graph.number_of_edges())

na = NetworkAnalyser()
na.analyse_network()
