import networkx.algorithms.community as nxcom
import time

class CommunityDetector:
    def detect_communities(self, graph):
        print('starting community detection using asynchronous label propagation:')
        start = time.time()
        communities = nxcom.asyn_lpa_communities(graph)
        n = 0
        print_communities = []
        for community in communities:
            n += 1
            print_communities.append(community)
        diff = time.time() - start
        print('community detection took: ' + str(diff))

        print('found ' + str(n) + ' communities')
        print('')
        print('following communities: ')

        for community in print_communities:
            print(str(community))

        return graph