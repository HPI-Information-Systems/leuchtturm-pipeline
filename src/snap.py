"""This module wraps cpp snap binaries and makes their execution available from python."""

import os
import subprocess


class Snap:
    """This class contains the information about the snap binaries and holds the functionality to execute them."""

    BIN_BIGCLAM = os.path.abspath('./libs/snap/bigclam')
    BIN_CENTRALITY = os.path.abspath('./libs/snap/centrality')
    BIN_COMMUNITY = os.path.abspath('./libs/snap/community')
    BIN_ROLX = os.path.abspath('./libs/snap/testrolx')

    TMP_FOLDER = os.path.abspath('./snap-tmp/')
    TMP_FILE_EDGES = 'edges'
    TMP_FILE_OUTPUT = 'out'

    def __init__(self, graph, quiet=False):
        """Initialize the Snap Interface. Input data has to be networkX's Graph format.

        Param 'quiet' toggles printing.
        """
        self.graph = graph
        self.quiet = quiet

        print('Number of Nodes: ' + str(len(self.graph.nodes())))

        if not os.path.exists(self.TMP_FOLDER):
            os.makedirs(self.TMP_FOLDER)

    def _write_input(self):
        with open(os.path.join(self.TMP_FOLDER, self.TMP_FILE_EDGES), 'w') as f:
            for edge in self.graph.edges():
                if edge[0] != edge[1]:
                    f.write(str(edge[0]) + '\t' + str(edge[1]) + '\n')
                    
    def _read_result(self):
        with open(os.path.join(self.TMP_FOLDER, self.TMP_FILE_OUTPUT), 'r') as f:
            for line in f:
                if line.startswith('#'):
                    if not self.quiet:
                        print(line, end='')
                else:
                    yield line.replace('\n', '').split('\t')

    def _exec(self, binary, args=None, use_default_args=True):
        command = [binary]
        if use_default_args:
            command += [
                '-i:' + os.path.join(self.TMP_FOLDER, self.TMP_FILE_EDGES),
                '-o:' + os.path.join(self.TMP_FOLDER, self.TMP_FILE_OUTPUT)
            ]
        if args is not None:
            command += args

        process = subprocess.Popen(command,
                                   stderr=subprocess.DEVNULL if self.quiet else None,
                                   stdout=subprocess.DEVNULL if self.quiet else None,
                                   cwd=self.TMP_FOLDER)
        process.wait()

        assert not process.returncode, 'ERROR: non-zero exit code ({})!'.format(process.returncode)

    def centrality(self):
        """Execute snap's centrality detection."""
        self._write_input()
        self._exec(self.BIN_CENTRALITY)

        for result in self._read_result():
            yield {
                'node_id': int(result[0]),
                'degree': int(result[1]),
                'closeness': int(result[2]),
                'betweenness': int(result[3]),
                'eigenvector': int(result[4]),
                'networkconstraint': int(result[5]),
                'clusterincoefficient': int(result[6]),
                'pagerank': int(result[7]),
                'hubscore': int(result[8]),
                'authorityscore': int(result[9])
            }

    def communities(self, algorithm=2):
        """:param algorithm: 1:Girvan-Newman, 2:Clauset-Newman-Moore, 3:Infomap (-a:)=2.

        :return:
        """
        self._write_input()
        self._exec(self.BIN_COMMUNITY, ['-a:{}'.format(algorithm)])

        for result in self._read_result():
            yield {
                'node_id': int(result[0]),
                'community': int(result[1])
            }

    def rolx(self):
        """Execute rolx."""
        self._write_input()
        self._exec(self.BIN_ROLX)

        for result in self._read_result():
            if len(result) == 2:
                yield {
                    'node_id': int(result[0]),
                    'role': int(result[1]),
                }

    # def bigclam(self):
    #     """Execute bigclam."""
    #     self._write_input()
    #     self._exec(self.BIN_BIGCLAM)

    #     for result in self._read_result():
    #          yield result
