"""This Module makes the network analysis functionality available."""

import py2neo
import networkx as nx
import matplotlib.pyplot as plt
import operator
from social_hierarchy_detection import SocialHierarchyDetector


class NetworkAnalyser:
    """This class holds the network functionality and gets the needed data.

    Initialize with needed parameters for solr_url, neo4j_host, http_port, bolt_port (for neo4j)
    """

    def __init__(self,
                 solr_url='http://sopedu.hpi.uni-potsdam.de:8983/solr/emails',
                 neo4j_host= 'http://172.16.64.28',  # 'http://sopedu.hpi.uni-potsdam.de',
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
        edges = neo_connection.run('MATCH (source)-[r]->(target) WHERE ()-->(source)-->() AND ()-->(target)-->() \
                                    RETURN id(source), id(target), size(r.mail_list) as cnt, r.time_list as tml')
        nodes = neo_connection.run('MATCH (p:Person) RETURN id(p), p.name, p.email')

        graph = nx.DiGraph()
        for edge in edges:
            graph.add_edge(edge['id(source)'], edge['id(target)'], volume=edge['cnt'], timeline=edge['tml'])

        for node in nodes:
            graph.add_node(node['id(p)'], name='|T|I|M|'.join(node['p.name']), email=node['p.email'])
        print(len(graph.nodes))
        print(len(graph.edges))
        social_hierarchy_detector = SocialHierarchyDetector(self.solr_url)
        social_hierarchy_scores = social_hierarchy_detector.detect_social_hierarchy(graph)
        # social_hierarchy_scores = {
        #     1789: 9.124392619010068, 580: 12.240280965335609, 1081: 26.37987980807487, 1816: 9.145741243918646,
        #     1064: 15.152177841396314, 1114: 15.177581770488784, 963: 23.667776814550674, 800: 0.025463857506665463,
        #     803: 12.188869698846476, 825: 6.257739454711132, 831: 9.781444715376796, 885: 9.514021758852707,
        #     900: 18.431642952354263, 982: 9.230725026500606, 2183: 9.10207433356118, 1049: 0.04337863968422003,
        #     2128: 9.079217298991635, 1110: 0.0020791866860019144, 1135: 9.161629985203534, 1323: 13.321351762179951,
        #     2742: 9.067835504621918, 1492: 0.03736859295873761, 1543: 4.557505229810029, 1673: 9.093374266557147,
        #     1760: 0.019437401197172747, 1878: 4.575436016971585, 1877: 4.575436016971585, 2201: 4.575436016888384,
        #     1919: 13.63277835046198, 1976: 9.093374266557147, 2072: 0.17968440078391315, 2114: 0.019437401197172747,
        #     2777: 0.0018654467347532571, 3126: 0.01015105652432728
        # }

        self._run_statistics(graph, social_hierarchy_scores)
        # social_hierarchy_labels = []
        # for node, score in social_hierarchy_scores.items():
        #     social_hierarchy_labels.append({'node_id': node, 'hierarchy': score})
        #     print(social_hierarchy_labels)
        #self.update_network(social_hierarchy_labels)
        # nx.write_graphml(graph, "enron.graphml")

    def _run_statistics(self, graph, hierarchy_scores):
        sorted_hierarchy = sorted(hierarchy_scores.values())
        top_five = sorted(hierarchy_scores, key=hierarchy_scores.get, reverse=True)[:5]
        email_addresses = nx.get_node_attributes(graph, 'email')
        for node in top_five:
            print((hierarchy_scores[node], email_addresses[node], node))

        # plot line diagram
        x = y = sorted_hierarchy
        plt.plot(x,y, '.-')
        plt.title('Hierarchy scores')
        plt.xlabel('Scores')
        plt.ylabel('Hierarchy values')

        # plot histogram
        plt.figure()
        num_bins = 40
        n, bins, patches = plt.hist(x, num_bins, alpha=0.75)
        plt.title('Distribution of Hierarchy scores')
        plt.xlabel('Scores')
        plt.ylabel('Hierarchy values')

        plt.rcParams['axes.axisbelow'] = True
        axes = plt.gca()
        axes.get_yaxis().grid(color='gray', linestyle='dashed')

        plt.show()

        # print(sorted_hierarchy)

    def _update_network(self, social_hierarchy_labels):
        """Update neo4j's data with the detected labels."""
        if social_hierarchy_labels:
            print('---------------- finished network analysis ----------------')

        neo_connection = py2neo.Graph(self.neo4j_host, http_port=self.http_port, bolt_port=self.bolt_port)
        neo_connection.run('UNWIND $hierarchy_scores AS scores '
                           'MATCH (node) WHERE ID(node) = scores.node_id '
                           'SET node.hierarchy = scores.hierarchy', hierarchy_scores=social_hierarchy_labels)
        print('- finished upload of hierarchy labels.')


na = NetworkAnalyser()
na.analyse_network()
