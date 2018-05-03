import networkx.algorithms.community as nxcom
import time
from snap import Snap

class CommunityDetector:
    def clauset_newman_moore(self, graph):
        print('starting community detection using clauset-newman-moore:')
        # start = time.time()
        snap = Snap(graph, quiet=False)

        for labelled_node in snap.communities(algorithm=2):
            yield labelled_node

    def girvan_newman(self, graph):
        print('starting community detection using girvan-newman:')
        # start = time.time()
        snap = Snap(graph, quiet=False)

        for labelled_node in snap.communities(algorithm=1):
            yield labelled_node

    def bigclam(self, graph):
        print('starting community detection using bigclam:')
        # start = time.time()
        snap = Snap(graph, quiet=False)

        for labelled_node in snap.bigclam():
            print(str(labelled_node))

        return graph