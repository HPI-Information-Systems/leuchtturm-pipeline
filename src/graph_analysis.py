import py2neo
import networkx as nx


class GraphAnalyser:

    def analyse_graph(self):
        neo_connection = py2neo.Graph()
        edges = neo_connection.run('MATCH (source)-[r]->(target) RETURN id(source), id(target)')

        graph = nx.Graph()

        for edge in edges:
            graph.add_edge(edge['id(source)'], edge['id(target)'])

        graph = self.detect_communities(graph)
        graph = self.detect_social_hierarchy(graph)
        self.upload_graph(graph)


    def detect_communities(self, graph):
        communities = nx.algorithms.community.centrality.girvan_newman(graph)
        print('communities:')
        n = 0
        for community in communities:
            n += 1
            print(n)

    def detect_social_hierarchy(self, graph):
        cliques = nx.enumerate_all_cliques(graph)
        print('cliques:')
        n = 0
        for clique in cliques:
            n += 1
            print(n)

    def upload_graph(self, graph):
        print(graph.number_of_nodes())
        print(graph.number_of_edges())
    