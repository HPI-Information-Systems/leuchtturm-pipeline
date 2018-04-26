import networkx as nx


class CommunityDetector:
    def detect_communities(self, graph):
        communities = nx.algorithms.community.centrality.girvan_newman(graph)
        print('communities:')
        n = 0
        for community in communities:
            n += 1
            print(n)

        return graph