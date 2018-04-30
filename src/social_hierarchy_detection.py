"""Module for the social hierarchy score."""
import networkx as nx
import time


class SocialHierarchyDetector:
    """Class for the social hierarchy score."""

    def __init__(self,
                 solr_url='http://sopedu.hpi.uni-potsdam.de:8983/solr/emails'):
        """Set solr config and path where rdd is read from."""
        self.solr_url = solr_url

    def detect_social_hierarchy(self, graph):
        """Trigger social hierarchy score detection."""
        number_of_cliques, cliques = self._number_of_cliques(graph)
        # raw_clique_score = self._raw_clique_score(graph, cliques)
        # betweenness_values = self.betweenness_centrality(graph)
        # mean_shortest_paths = self.mean_shortest_paths(graph)
        return graph

    def _number_of_cliques(self, graph):
        print('Start counting cliques')
        start = time.time()
        cliques = list(nx.find_cliques(graph))
        n = 0
        for clique in cliques:
            n += 1
        metric = dict()
        for node in graph.nodes:
            count = 0
            for clique in cliques:
                if node in clique:
                    count += 1
            metric[node] = count
        end = time.time()
        print(metric)
        print('Found ' + str(n) + ' cliques, took: ' + str(end - start) + 's')
        return metric, cliques

    def _raw_clique_score(self, graph, cliques):
        print('Start computing raw clique score')
        start = time.time()
        metric = dict()
        for node in graph.nodes:
            score = 0
            for clique in cliques:
                if node in clique:
                    size = len(clique)
                    score += 2 ** (size - 1)
            metric[node] = score
        end = time.time()
        print(metric)
        print('Found ' + len(graph.nodes) + ' raw clique scores, took: ' + str(end - start) + 's')
        return metric

    def _betweenness_centrality(self, graph):
        print('Start calculating betweenness values')
        start = time.time()
        betweenness_values = nx.betweenness_centrality(graph)
        n = 0
        for value in betweenness_values:
            n += 1
        end = time.time()
        print('Calculated ' + str(n) + ' betweenness values, took: ' + str(end - start) + 's')
        return betweenness_values

    def _mean_shortest_paths(self, graph):
        print('Start calculating mean shortest paths')
        start = time.time()
        table_of_means = dict()
        for node in graph.nodes:
            shortest_paths = nx.single_source_shortest_path_length(graph, node)
            mean = sum(shortest_paths.values()) / len(shortest_paths)
            amount_neighbors = len(nx.descendants(graph, node))
            if amount_neighbors is not 0:  # should not happen as we pre-delete leave nodes
                mean /= amount_neighbors
            else:
                mean = 1
            table_of_means[node] = mean
        n = 0
        for entry in table_of_means:
            n += 1
        end = time.time()
        print('Calculated ' + str(n) + ' mean shortest paths, took: ' + str(end - start) + 's')
        print(table_of_means[max(table_of_means, key=table_of_means.get)])
        print(table_of_means[min(table_of_means, key=table_of_means.get)])
        return table_of_means
