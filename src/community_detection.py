"""This Module makes the community detection methods available."""

from snap import Snap


class CommunityDetector:
    """This class holds the Community detection methods."""

    def clauset_newman_moore(self, graph):
        """Detect communities using clauset-newman-moore from snap.

        Input: networkX graph
        """
        print('starting community detection using clauset-newman-moore:')
        snap = Snap(graph, quiet=False)

        community_labels = []
        for labelled_node in enumerate(snap.communities(algorithm=2)):
            community_labels.append(labelled_node[1])

        return community_labels

    def girvan_newman(self, graph):
        """Detect communities using girvan-newman from snap.

        Input: networkX graph
        """
        print('starting community detection using girvan-newman:')
        snap = Snap(graph, quiet=False)

        community_labels = []
        for labelled_node in enumerate(snap.communities(algorithm=1)):
            community_labels.append(labelled_node[1])

        return community_labels

    def bigclam(self, graph):
        """Detect communities using bigclam from snap.

        Input: networkX graph
        """
        print('starting community detection using bigclam:')
        snap = Snap(graph, quiet=False)

        community_labels = []
        for labelled_node in enumerate(snap.bigclam()):
            community_labels.append(labelled_node[1])

        return community_labels
