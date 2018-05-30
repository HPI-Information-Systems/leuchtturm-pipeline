"""This Module makes the network analysis functionality available."""

import py2neo
import networkx as nx
import numpy as np
import json
import matplotlib.pyplot as plt
from community_detection import CommunityDetector
from role_detection import RoleDetector
from social_hierarchy_detection import SocialHierarchyDetector


class NetworkAnalyser:
    """This class holds the network functionality and gets the needed data.

    Initialize with needed parameters for solr_url, neo4j_host, http_port, bolt_port (for neo4j)
    """

    def __init__(self,
                 solr_url='http://sopedu.hpi.uni-potsdam.de:8983/solr/emails',
                 neo4j_host='http://172.16.64.28',  # 'http://sopedu.hpi.uni-potsdam.de',
                 http_port=61100,
                 bolt_port=61000):
        """Set solr config and path where rdd is read from."""
        self.solr_url = solr_url
        self.neo4j_host = neo4j_host
        self.http_port = http_port
        self.bolt_port = bolt_port

    def _build_graph(self):
        """Fetch data from neo4j and build graph."""
        neo_connection = py2neo.Graph(self.neo4j_host, http_port=self.http_port, bolt_port=self.bolt_port)
        edges = neo_connection.run('MATCH (source)-[r]->(target) \
                                    RETURN id(source), id(target), size(r.mail_list) as cnt, r.time_list as tml')
        nodes = neo_connection.run('MATCH (p:Person) RETURN id(p), p.email_addresses, \
            p.identifying_name, p.hierarchy')

        digraph = nx.DiGraph()

        for edge in edges:
            digraph.add_edge(edge['id(source)'], edge['id(target)'], volume=edge['cnt'], timeline=edge['tml'])

        for node in nodes:
            if not node['p.identifying_name'] == '':
                digraph.add_node(node['id(p)'], email=node['p.email_addresses'], hierarchy=node['p.hierarchy'])

        graph = digraph.to_undirected()
        print('Number of Nodes: ' + str(graph.number_of_nodes()))
        print('Number of Edges: ' + str(graph.number_of_edges()))

        return graph, digraph

    def analyse_network(self, upload=False):
        """Analyse the network. Parameter upload decides if data in neo4j will be updated."""
        graph, digraph = self._build_graph()

        social_hierarchy_detector = SocialHierarchyDetector()
        social_hierarchy_labels = social_hierarchy_detector.detect_social_hierarchy(digraph, graph)
        self._save_results_locally(social_hierarchy_labels, 'hierarchy.json')

        community_detector = CommunityDetector(graph)
        community_labels = community_detector.clauset_newman_moore()
        self._save_results_locally(community_labels, 'community.json')

        role_detector = RoleDetector()
        role_labels = role_detector.rolx(graph)
        self._save_results_locally(role_labels, 'role.json')

        if upload:
            self.update_network(community_labels, "community")
            self.update_network(role_labels, "role")
            self.update_network(social_hierarchy_labels, "hierarchy")

    def run_statistics(self):
        """Run statistics on hierarchy values in neo4j."""
        graph, __ = self._build_graph()
        self._run_statistics(graph)

    def _save_results_locally(self, dictionary, filename):
        with open(filename, 'w') as fp:
            json.dump(dictionary, fp)

    def _run_statistics(self, graph):
        hierarchy_scores = nx.get_node_attributes(graph, 'hierarchy')
        sorted_hierarchy = sorted(hierarchy_scores.values())
        top_five = sorted(hierarchy_scores, key=hierarchy_scores.get, reverse=True)[:5]
        email_addresses = nx.get_node_attributes(graph, 'email')
        for node in top_five:
            print((hierarchy_scores[node], email_addresses[node], node))

        # plot line diagram
        y = sorted_hierarchy
        x = np.random.randint(0, high=100, size=len(y))
        # plt.plot(x, y, '.-')
        # plt.title('Hierarchy scores')
        # plt.xlabel('Scores')
        # plt.ylabel('Hierarchy values')

        # plot histogram
        plt.figure()
        num_bins = 3
        n, bins, patches = plt.hist(y, num_bins, alpha=0.75)
        plt.title('Distribution of Hierarchy Scores')
        plt.xlabel('Scores')
        plt.ylabel('Hierarchy values')
        plt.rcParams['axes.axisbelow'] = True
        axes = plt.gca()
        axes.get_yaxis().grid(color='gray', linestyle='dashed')
        plt.savefig('three_bins_enron_dev.png', bbox_inches='tight')

        # plot histogram
        plt.figure()
        num_bins = 5
        n, bins, patches = plt.hist(y, num_bins, alpha=0.75)
        plt.title('Distribution of Hierarchy scores')
        plt.xlabel('Scores')
        plt.ylabel('Hierarchy values')
        plt.rcParams['axes.axisbelow'] = True
        axes = plt.gca()
        axes.get_yaxis().grid(color='gray', linestyle='dashed')
        plt.savefig('five_bins_enron_dev.png', bbox_inches='tight')

        # plot boxplot diagram
        fig1, ax1 = plt.subplots()
        ax1.set_title('Distribution of Hierarchy Scores')
        ax1.boxplot(y)
        ax1.axes.xaxis.set_ticklabels([])
        fig1.savefig('boxplot_enron_dev.png', bbox_inches='tight')

        # plot scatter plot
        fig, ax = plt.subplots()
        ax.scatter(x, y, c=y, alpha=1, cmap='rainbow')
        ax.set_title('Distribution of Hierarchy Scores')
        ax.axes.xaxis.set_ticklabels([])
        fig.savefig('scatter_plot_enron_dev.png', bbox_inches='tight')

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
na.analyse_network(upload=False)
# na.run_statistics()
