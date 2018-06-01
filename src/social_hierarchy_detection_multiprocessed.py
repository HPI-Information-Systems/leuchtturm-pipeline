"""Module for the social hierarchy score."""
import networkx as nx
import time
import pandas as pd
from datetime import datetime, timedelta
from pandas.tseries.offsets import BDay
from multiprocessing import Process, Queue


def _three_business_days(timestamp):
    return (pd.to_datetime(timestamp, unit='s') + BDay(3)).timestamp()


class SocialHierarchyDetector:
    """Class for the social hierarchy score."""

    def detect_social_hierarchy(self, graph, undirected_graph):
        """
        Trigger social hierarchy score detection.

        Returns dict of nodes as keys and their hierarchy scores as values.
        """
        print(datetime.now(), 'lt_logs', 'Start detecting social hierarchy scores', flush=True)
        start = time.time()
        queue = Queue()
        own_queue = Queue()
        jobs = []

        # TODO Add betweenness centrality (left out now for better performance)

        process_number_of_emails = Process(target=self._number_of_emails, args=(graph, queue))
        process_number_of_emails.start()
        jobs.append(process_number_of_emails)

        process_scores_and_avg_times = Process(target=self._response_score_and_average_time, args=(graph, queue))
        process_scores_and_avg_times.start()
        jobs.append(process_scores_and_avg_times)

        process_number_of_cliques = Process(target=self._number_of_cliques, args=(undirected_graph, queue))
        process_number_of_cliques.start()
        jobs.append(process_number_of_cliques)

        process_degree_values = Process(target=self._degree_centrality, args=(graph, queue))
        process_degree_values.start()
        jobs.append(process_degree_values)

        process_hub_authorities = Process(target=self._hubs_and_authorities, args=(graph, queue))
        process_hub_authorities.start()
        jobs.append(process_hub_authorities)

        process_clustering_coefficients = Process(target=self._clustering_coefficient, args=(undirected_graph, queue))
        process_clustering_coefficients.start()
        jobs.append(process_clustering_coefficients)

        process_mean_shortest_paths = Process(target=self._mean_shortest_paths, args=(graph, queue))
        process_mean_shortest_paths.start()
        jobs.append(process_mean_shortest_paths)

        cliques = None
        number_of_emails = None
        response_avg_times = None
        triggered = False

        while True:
            name = None
            metric = None
            try:
                name, metric = queue.get_nowait()
                own_queue.put((name, metric))
            except:
                pass
            if name == 'number_of_emails':
                number_of_emails = metric

            if name == 'cliques':
                cliques = metric

            if name == 'average_time':
                response_avg_times = metric

            if None not in [cliques, number_of_emails, response_avg_times] and not triggered:
                triggered = True
                process_weighted_clique_score = Process(target=self._weighted_clique_score, args=(graph,
                                                                                                  queue,
                                                                                                  cliques,
                                                                                                  number_of_emails,
                                                                                                  response_avg_times))
                process_weighted_clique_score.start()
                jobs.append(process_weighted_clique_score)

                process_raw_clique_score = Process(target=self._raw_clique_score, args=(graph, queue, cliques))
                process_raw_clique_score.start()
                jobs.append(process_raw_clique_score)

            running = False
            for process in jobs:
                if process.is_alive():
                    running = True

            if not running:
                break

        metrics = []
        while not own_queue.empty():
            name, metric = own_queue.get()
            print(name)
            if type(metric) is not dict:
                print('No dict')
                continue

            if name == 'average_time' or name == 'mean_shortest_paths':
                metrics.append(self._normalize(metric, high=False))
            else:
                metrics.append(self._normalize(metric))

        # metrics = []
        # for metric in [response_avg_times, mean_shortest_paths]:
        #     metrics.append(self._normalize(metric, high=False))
        #
        # for metric in [number_of_cliques, raw_clique_score, degree_values,
        #                hub_values, authority_values, number_of_emails, clustering_coefficients, weighted_clique_score]:
        #     metrics.append(self._normalize(metric))

        hierarchy_scores = self._aggregate(graph, metrics)
        # for testing purposes:
        # hierarchy_scores = {
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
        hierarchy_scores_formatted = self._format_for_upload(hierarchy_scores)

        end = time.time()
        print(datetime.now(),
              'lt_logs',
              'Calculated ' + str(len(graph.nodes)) + ' social hierarchy scores, took: ' + str(end - start) + 's',
              flush=True)
        return hierarchy_scores_formatted

    def _normalize(self, metric, high=True):
        inf = min(metric.values())
        sup = max(sorted(metric.values()))

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
            hierarchy_scores[node] = round(score)

        return hierarchy_scores

    def _format_for_upload(self, metric):
        scores_formatted = []
        for node, score in metric.items():
            scores_formatted.append({'node_id': node, 'hierarchy': score})

        return scores_formatted

    def _number_of_emails(self, graph, queue):
        print(datetime.now(), 'lt_logs', 'Start counting emails', flush=True)
        start = time.time()
        metric = dict()
        for node in graph.nodes:
            total_volume = 0
            neighbours = graph[node]
            for neighbour in neighbours:
                total_volume += neighbours[neighbour]['volume']
            metric[node] = total_volume
        end = time.time()
        print(datetime.now(),
              'lt_logs',
              'Found ' + str(len(graph.nodes)) + ' email volumes, took: ' + str(end - start) + 's',
              flush=True)

        queue.put(('number_of_emails', metric))

    def _response_score_and_average_time(self, graph, queue):
        print(datetime.now(), 'lt_logs', 'Start computing response scores and average time', flush=True)
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
                dif = datetime.fromtimestamp(response[1]) - datetime.fromtimestamp(response[0])
                total += timedelta.total_seconds(dif)
            if total == 0:
                avg_time = 432000  # five days max
            else:
                avg_time = total / len(responses)
                cnt += 1

            metric_average_time[node] = avg_time
        end = time.time()
        print(datetime.now(),
              'lt_logs',
              'Found ' + str(len(graph.nodes)) + ' response scores, took: ' + str(end - start) + 's',
              flush=True)

        queue.put(('response_score', metric_response_score))
        queue.put(('average_time', metric_average_time))

    def _clustering_coefficient(self, graph, queue):
        print(datetime.now(), 'lt_logs', 'Start calculating clustering coefficients', flush=True)
        start = time.time()
        clustering_values = nx.clustering(graph)
        n = len(clustering_values)
        end = time.time()
        print(datetime.now(),
              'lt_logs',
              'Found ' + str(n) + ' clustering coefficients, took: ' + str(end - start) + 's',
              flush=True)

        queue.put(('clustering_values', clustering_values))

    def _number_of_cliques(self, graph, queue):
        print(datetime.now(), 'lt_logs', 'Start counting cliques', flush=True)
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
        print(datetime.now(), 'lt_logs', 'Found ' + str(n) + ' cliques, took: ' + str(end - start) + 's', flush=True)
        queue.put(('cliques', cliques))
        queue.put(('number_of_cliques', metric))

    def _raw_clique_score(self, graph, queue, cliques):
        print(datetime.now(), 'lt_logs', 'Start computing raw clique score', flush=True)
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
        print(datetime.now(),
              'lt_logs',
              'Found ' + str(len(graph.nodes)) + ' raw clique scores, took: ' + str(end - start) + 's',
              flush=True)
        queue.put(('raw_clique_score', metric))

    def _weighted_clique_score(self, graph, queue, cliques, email_metric, response_metric):
        print(datetime.now(), 'lt_logs', 'Start computing weighted clique score', flush=True)
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
        print(datetime.now(),
              'lt_logs',
              'Found ' + str(len(graph.nodes)) + ' weighted clique scores, took: ' + str(end - start) + 's',
              flush=True)
        queue.put(('weighted_clique_score', metric))

    def _betweenness_centrality(self, graph, queue):
        print(datetime.now(), 'lt_logs', 'Start calculating betweenness values', flush=True)
        start = time.time()
        betweenness_values = nx.betweenness_centrality(graph)
        n = len(betweenness_values)
        end = time.time()
        print(datetime.now(),
              'lt_logs',
              'Calculated ' + str(n) + ' betweenness values, took: ' + str(end - start) + 's',
              flush=True)
        queue.put(('betweenness', betweenness_values))

    def _degree_centrality(self, graph, queue):
        print(datetime.now(), 'lt_logs', 'Start calculating degree values', flush=True)
        start = time.time()
        degree_values = nx.degree_centrality(graph)
        n = len(degree_values)
        end = time.time()
        print(datetime.now(),
              'lt_logs',
              'Calculated ' + str(n) + ' degree values, took: ' + str(end - start) + 's',
              flush=True)
        queue.put(('degree', degree_values))

    def _hubs_and_authorities(self, graph, queue):
        print(datetime.now(), 'lt_logs', 'Start calculating hubs and authorities values', flush=True)
        start = time.time()
        h_a_values = nx.hits(graph)
        n = len(h_a_values[0])
        end = time.time()
        print(datetime.now(),
              'lt_logs',
              'Calculated ' + str(n) + ' hubs and authorities values, took: ' + str(end - start) + 's',
              flush=True)
        queue.put(('hubs', h_a_values[0]))
        queue.put(('authorities', h_a_values[1]))

    def _mean_shortest_paths(self, graph, queue):
        print(datetime.now(), 'lt_logs', 'Start calculating mean shortest paths', flush=True)
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
        print(datetime.now(),
              'lt_logs',
              'Calculated ' + str(n) + ' mean shortest paths, took: ' + str(end - start) + 's',
              flush=True)
        queue.put(('mean_shortest_paths', table_of_means))
