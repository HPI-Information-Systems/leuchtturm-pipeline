import networkx.algorithms.community as nxcom
import time

class CommunityDetector:
    def detect_communities(self, graph):
        print('starting community detection using asynchronous label propagation:')
        start = time.time()
        communities = nxcom.asyn_lpa_communities(graph)
        diff = time.time() - start
        print('community detection took:' + diff)
        n = 0
        for community in communities:
            n += 1
        print('found ' + n + ' communities')

        return graph