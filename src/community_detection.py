"""This Module makes the community detection methods available."""

from .snap import Snap


class CommunityDetector:
    """This class holds the Community detection methods."""

    def __init__(self, graph):
        """Initialize."""
        self.snap = Snap(graph, quiet=False)

    def clauset_newman_moore(self):
        """Detect communities using clauset-newman-moore from snap.

        Input: networkX graph
        """
        print(datetime.now(), 'lt_logs', 'starting community detection using clauset-newman-moore:', flush=True)
        return self.run_snap_community_detection(self.snap.communities, 2)

    def girvan_newman(self):
        """Detect communities using girvan-newman from snap.

        Input: networkX graph
        """
        print(datetime.now(), 'lt_logs', 'starting community detection using girvan-newman:', flush=True)
        return self.run_snap_community_detection(self.snap.communities, 1)

    def bigclam(self):
        """Detect communities using bigclam from snap.

        Input: networkX graph
        """
        print(datetime.now(), 'lt_logs', 'starting community detection using bigclam:', flush=True)
        return self.run_snap_community_detection(self.snap.bigclam)

    def run_snap_community_detection(self, func, *args):
        """Detect communities using declared function from snap.

        Input: networkX graph, function, arguments for function
        """
        community_labels = []
        for labelled_node in enumerate(func(*args)):
            community_labels.append(labelled_node[1])

        return community_labels
