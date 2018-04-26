import py2neo
import networkx as nx


class GraphAnalyser:
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

    def analyse_graph(self):
        print(self.neo4j_host)
        neo_connection = py2neo.Graph(self.neo4j_host, http_port=self.http_port, bolt_port=self.bolt_port)
        edges = neo_connection.run('MATCH (source)-[r]->(target) RETURN id(source), id(target)')

        graph = nx.Graph()

        for edge in edges:
            graph.add_edge(edge['id(source)'], edge['id(target)'])

        #graph = self.detect_communities(graph)
        #graph = self.detect_social_hierarchy(graph)
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

ga = GraphAnalyser()
ga.analyse_graph()
