import py2neo
import networkx as nx


class SocialHierarchyDetector:
    def __init__(self,
                 solr_url='http://sopedu.hpi.uni-potsdam.de:8983/solr/emails'):
        """Set solr config and path where rdd is read from."""
        self.solr_url = solr_url

    def detect_social_hierarchy(self, graph):
        cliques = nx.enumerate_all_cliques(graph)
        print('cliques:')
        n = 0
        for clique in cliques:
            n += 1
        print(n)

        return graph
