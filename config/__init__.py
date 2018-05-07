from configparser import ConfigParser
import argparse
import logging
import os


class Config:
    DEFAULT_CONFIG_FILE = os.path.join(os.path.realpath(__file__), 'default.ini')
    DEFAULTS = {
        'settings': {
            'run_local': False,
            'log_level': 'INFO'
        },
        'data': {
            'dataset': 'DefaultDataset',
            'source_dir': './emails',
            'working_dir': './pipeline_result',
            'time_min': 0,
            'time_max': 2147483647
        },
        'solr': {
            'import': False,
            'host': '0.0.0.0',
            'port': '8983',
            'collection': 'default',
            'data_location': './data/solr',
            'log_location': './data/solr/log'
        },
        'neo4j': {
            'import': False,
            'host': '0.0.0.0',
            'port': '7474',
            'data_location': './data/neo4j',
            'log_location': './data/neo4j/log'
        }
    }

    def __init__(self):
        logging.basicConfig(format='%(asctime)s.%(msecs)03d|%(name)s|%(levelname)s> %(message)s', datefmt='%H:%M:%S')
        self.logger = logging.getLogger()

        conf_parser, self.conf_file = self._get_cli_conf_file()
        self.config = self._load_conf_file()

        self.args = self._get_cli_args(conf_parser)
        self.args = vars(self.args)

        self.logger.setLevel(self.get('settings', 'log_level'))

        self._print_info()

    def get(self, section, option):
        arg = self.args.get(section + '_' + option, None)
        if arg is None:
            return self.config.get(section, option)
        return arg

    def _print_info(self):
        self.logger.info(self.args)

        self.logger.info(('  Neo4j:\n'
                          '=========\n'
                          'Go to the neo4j directory and edit config/neo4j.conf\n'
                          '  dbms.directories.data={}\n'
                          '  dbms.directories.logs={}\n'
                          '  dbms.connectors.default_listen_address={}\n'
                          'Then start neo4j:'
                          '  $ ./bin/neo4j start\n'
                          '\n').format(self.get('neo4j', 'data_location'),
                                       self.get('neo4j', 'log_location'),
                                       self.get('neo4j', 'host')))

        self.logger.info(('  Solr:\n'
                          '=========\n'
                          'Go to the solr directory run\n'
                          '  $ ./bin/solr start -h {} -s {} -p {}\n'
                          '\n').format(self.get('solr', 'host'),
                                       self.get('solr', 'data_location'),
                                       self.get('solr', 'port')))

    def _load_conf_file(self):
        config = ConfigParser()
        config.read_dict(self.DEFAULTS)
        config.read(self.conf_file)
        return config

    def _get_cli_conf_file(self):
        conf_parser = argparse.ArgumentParser(add_help=False)
        conf_parser.add_argument('-c', '--conf_file',
                                 help='Location of config file containing default settings',
                                 metavar='FILE',
                                 default=self.DEFAULT_CONFIG_FILE)
        args, _ = conf_parser.parse_known_args()
        return conf_parser, args.conf_file

    def _get_cli_args(self, conf_parser):
        parser = argparse.ArgumentParser(
            parents=[conf_parser],
            description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter
        )

        parser.add_argument('--settings-log-level', action='store',
                            choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET'],
                            help='Verbosity level of logging', default=None)
        parser.add_argument('--settings-run-local', action='store_true', default=None,
                            help='Set this to execute the pipeline in a local environment')

        parser.add_argument('--data-source-dir',
                            help='Path to directory where raw emails are located.')
        parser.add_argument('--data-working-dir',
                            help='Path where results will be written to. WARNING: THIS DIRECTORY SHOULD NOT EXIST!')

        parser.add_argument('--solr-import', action='store_true',
                            help='Set this flag if results should be written to solr.')
        parser.add_argument('--solr-host',
                            help='hostname of solr instance')
        parser.add_argument('--solr-port',
                            help='port of solr instance')
        parser.add_argument('--solr_collection',
                            help='solr collection to be used')
        parser.add_argument('--solr-data-location',
                            help='solr data directory (used to start solr)')
        parser.add_argument('--solr-log-location',
                            help='solr log directory (used to start solr)')

        parser.add_argument('--neo4j-import', action='store_true',
                            help='Set this flag if results should be written to neo4j.')
        parser.add_argument('--neo4j-host',
                            help='hostname of neo4j instance')
        parser.add_argument('--neo4j-port',
                            help='port of neo4j instance')
        parser.add_argument('--neo4j-data-location',
                            help='neo4j data directory (used to start neo4j)')
        parser.add_argument('--neo4j-log-location',
                            help='neo4j log directory (used to start neo4j)')

        return parser.parse_args()


if __name__ == '__main__':
    conf = Config()
