"""This Module makes the role detection methods available."""

from .snap import Snap


class RoleDetector:
    """This class holds the role detection method."""

    def rolx(self, graph):
        """Detect roles using rolx from snap.

        Input: networkX graph
        """
        print(datetime.now(), 'lt_logs', 'starting role detection using rolx:', flush=True)
        snap = Snap(graph, quiet=False)
        role_labels = []
        for labelled_node in enumerate(snap.rolx()):
            role_labels.append(labelled_node[1])

        return role_labels
