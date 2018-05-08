"""Module for the social hierarchy score."""
import networkx as nx
import time
import datetime
import pandas as pd
from pandas.tseries.offsets import BDay


def _three_business_days(timestamp):
    return (pd.to_datetime(timestamp, unit='s') + BDay(3)).timestamp()


class SocialHierarchyDetector:
    """Class for the social hierarchy score."""

    def __init__(self,
                 solr_url='http://sopedu.hpi.uni-potsdam.de:8983/solr/emails'):
        """Set solr config and path where rdd is read from."""
        self.solr_url = solr_url

    def detect_social_hierarchy(self, graph):
        """Trigger social hierarchy score detection."""
        print('Start detecting social hierarchy scores')
        start = time.time()
        number_of_emails = self._number_of_emails(graph)
        response_scores, response_avg_times = self._response_score_and_average_time(graph)
        number_of_cliques, cliques = self._number_of_cliques(graph)
        raw_clique_score = self._raw_clique_score(graph, cliques)
        weighted_clique_score = self._weighted_clique_score(graph, cliques, number_of_emails, response_avg_times)
        betweenness_values = self._betweenness_centrality(graph)
        degree_values = self._degree_centrality(graph)
        hub_values, authority_values = self._hubs_and_authorities(graph)
        clustering_coefficients = self._clustering_coefficient(graph)
        mean_shortest_paths = self._mean_shortest_paths(graph)

        metrics = []
        for metric in [response_avg_times, mean_shortest_paths]:
            metrics.append(self._normalize(metric, high=False))

        for metric in [number_of_cliques, raw_clique_score, betweenness_values, degree_values,
                       hub_values, authority_values, number_of_emails, clustering_coefficients, weighted_clique_score]:
            metrics.append(self._normalize(metric))

        graph = self._aggregate(graph, metrics)
        end = time.time()
        print('Calculated ' + str(len(graph.nodes)) + ' social hierarchy scores, took: ' + str(end - start) + 's')
        return graph

    def _normalize(self, metric, high=True):
        inf = min(metric.values())  # find_min_max(metric)
        sup = max(sorted(metric.values())[:-1])

        normalized_metric = dict()
        for key, value in metric.items():
            normalized_value = 100 * (value - inf) / (sup - inf)
            if not high:
                normalized_value = 100 - normalized_value
            normalized_metric[key] = normalized_value

        return normalized_metric

    def _aggregate(self, graph, metrics):
        hierarchy_scores = dict()
        for node in graph.nodes:
            score = 0
            for metric in metrics:
                score += metric[node]
            score = score / len(metrics)
            hierarchy_scores[node] = score

        for i in range(1, 10):
            print(max(sorted(list(hierarchy_scores.values()))[:-i]))
        print(min(hierarchy_scores.values()))
        nx.set_node_attributes(graph, hierarchy_scores, 'hierarchy')
        return graph

    def _number_of_emails(self, graph):
        print('Start counting emails')
        start = time.time()
        metric = dict()
        for node in graph.nodes:
            total_volume = 0
            neighbours = graph[node]
            for neighbour in neighbours:
                total_volume += neighbours[neighbour]['volume']
            metric[node] = total_volume
        end = time.time()
        print('Found ' + str(len(graph.nodes)) + ' email volumes, took: ' + str(end - start) + 's')
        return metric

    def _response_score_and_average_time(self, graph):
        print('Start computing response scores and average time')
        # graph = graph.to_directed()
        start = time.time()
        metric_response_score = dict()
        metric_average_time = dict()
        cnt = 0
        for node in graph.nodes:
            responses = []
            unresponsed = []
            neighbours = graph[node]
            for neighbour in neighbours:
                timestamps_to_neighbour = neighbours[neighbour]['timeline']
                timestamps_from_neighbour = []
                try:
                    timestamps_from_neighbour = graph.edges[neighbour, node]['timeline']
                except Exception:
                    pass

                if timestamps_from_neighbour and timestamps_to_neighbour[0] == timestamps_from_neighbour[0]:
                    continue  # take out loops

                for t1 in timestamps_to_neighbour:
                    for t2 in timestamps_from_neighbour:
                        if t2 > t1 and t2 < _three_business_days(t1):
                            responses.append((t1, t2))
                        else:
                            unresponsed.append(t1)

            response_score = len(responses) / (len(responses) + len(unresponsed) + 1)
            metric_response_score[node] = response_score

            total = 0
            for response in responses:
                dif = datetime.datetime.fromtimestamp(response[1]) - datetime.datetime.fromtimestamp(response[0])
                total += datetime.timedelta.total_seconds(dif)
            if total == 0:
                avg_time = 432000  # five days max
            else:
                avg_time = total / len(responses)
                cnt += 1

            metric_average_time[node] = avg_time
        end = time.time()
        print('Found ' + str(len(graph.nodes)) + ' response scores, took: ' + str(end - start) + 's')
        return metric_response_score, metric_average_time

    def _clustering_coefficient(self, graph):
        print('Start calculating clustering coefficients')
        start = time.time()
        graph = graph.to_undirected()
        clustering_values = nx.clustering(graph)
        n = len(clustering_values)
        end = time.time()
        print('Found ' + str(n) + ' clustering coefficients, took: ' + str(end - start) + 's')
        return clustering_values

    def _number_of_cliques(self, graph):
        print('Start counting cliques')
        start = time.time()
        graph = graph.to_undirected()
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
        print('Found ' + str(n) + ' cliques, took: ' + str(end - start) + 's')
        return metric, cliques

    def _raw_clique_score(self, graph, cliques):
        print('Start computing raw clique score')
        start = time.time()
        graph = graph.to_undirected()
        metric = dict()
        for node in graph.nodes:
            score = 0
            for clique in cliques:
                if node in clique:
                    size = len(clique)
                    score += 2 ** (size - 1)
            metric[node] = score
        end = time.time()
        print('Found ' + str(len(graph.nodes)) + ' raw clique scores, took: ' + str(end - start) + 's')
        return metric

    def _weighted_clique_score(self, graph, cliques, email_metric, response_metric):
        print('Start computing weighted clique score')
        start = time.time()
        metric = dict()
        for node in graph.nodes:
            score = 0
            for clique in cliques:
                if node in clique:
                    size = len(clique)
                    time_score = 0
                    for other_node in clique:
                        email_volume = email_metric[other_node]
                        response_value = response_metric[other_node]
                        time_score += email_volume * response_value
                    score += time_score * (2 ** (size - 1))
            metric[node] = score
        end = time.time()
        print('Found ' + str(len(graph.nodes)) + ' weighted clique scores, took: ' + str(end - start) + 's')
        return metric

    def _betweenness_centrality(self, graph):
        print('Start calculating betweenness values')
        start = time.time()
        betweenness_values = nx.betweenness_centrality(graph)
        n = len(betweenness_values)
        end = time.time()
        print('Calculated ' + str(n) + ' betweenness values, took: ' + str(end - start) + 's')
        return betweenness_values

    def _degree_centrality(self, graph):
        print('Start calculating degree values')
        start = time.time()
        degree_values = nx.degree_centrality(graph)
        n = len(degree_values)
        end = time.time()
        print('Calculated ' + str(n) + ' degree values, took: ' + str(end - start) + 's')
        return degree_values

    def _hubs_and_authorities(self, graph):
        print('Start calculating hubs and authorities values')
        start = time.time()
        h_a_values = nx.hits(graph)
        n = len(h_a_values)
        end = time.time()
        print('Calculated ' + str(n) + ' hubs and authorities values, took: ' + str(end - start) + 's')
        return h_a_values[0], h_a_values[1]

    def _mean_shortest_paths(self, graph):
        print('Start calculating mean shortest paths')
        start = time.time()
        table_of_means = dict()
        for node in graph.nodes:
            shortest_paths = nx.single_source_shortest_path_length(graph, node)
            mean = sum(shortest_paths.values()) / len(shortest_paths)
            amount_neighbors = len(nx.descendants(graph, node))
            if amount_neighbors is not 0:
                mean /= amount_neighbors
            else:  # should not happen as we pre-delete leave nodes
                mean = 1
            table_of_means[node] = mean
        n = len(table_of_means)
        end = time.time()
        print('Calculated ' + str(n) + ' mean shortest paths, took: ' + str(end - start) + 's')
        return table_of_means
