"""Global configuration handler for everything."""

from configparser import ConfigParser, ExtendedInterpolation
import argparse
# import logging


class Config:
    """Docstring.

    Global configuration handler for everything.

    to override command line arguments, provide list during initialisation.
    This can also be done in places, where the program isn't executed directly from the command line.
    """

    DEFAULT_CONFIG_FILE = './config/default.ini'
    DEFAULTS = {
        'settings': {
            'log_level': 'INFO'
        },
        'data': {
            'dataset': 'dataset',
            'source_dir': './data/input',
            'working_dir': './data/processed',
            'results_dir': '${data:working_dir}/pipeline_results',
            'results_correspondent_dir': '${results_dir}_correspondent',
            'results_injected_dir': '${results_dir}_injected',
            'results_topics_dir': '${results_dir}_topics',
            'time_min': 0,
            'time_max': 2147483647
        },
        'solr': {
            'import': False,
            'protocol': 'http',
            'host': '0.0.0.0',
            'port': 8983,
            'url_path': 'solr',
            'collection': '${data:dataset}',
            'topic_collection': '${collection}_topics',
            'data_location': './data/solr/',
            'log_location': './data/logs/solr'
        },
        'neo4j': {
            'import': False,
            'protocol': 'http',
            'host': '0.0.0.0',
            'http_port': 7474,
            'bolt_port': 7687,
            'data_location': './data/neo4j',
            'log_location': './data/logs/neo4j',
            'create_node_index': True
        },
        'spark': {
            'driver_memory': '6g',
            'executor_memory': '4g',
            'run_local': False,
            'num_executors': 23,
            'executor_cores': 4,
            'parallelism': 276
        },
        'models': {
            'directory': './models'
        },
        'tm_preprocessing': {
            'buckets_dir': '${data:working_dir}/tm_buckets',
            'bucket_timeframe': 'month',
            'minimum_total_word_document_frequency': 3,
            'maximum_fraction_word_document_frequency': 0.1,
            'file_removed_frequent_words': '${data:working_dir}/removed_frequent_words_${data:dataset}.txt',
            'file_removed_infrequent_words': '${data:working_dir}/removed_infrequent_words_${data:dataset}.txt'
        },
        'topic_modelling': {
            'train_model': True,
            'iterations': 1000,
            'num_topics': 100,
            'alpha_numerator': 50,
            'eta': 0.1,
            'file_model': '${models:directory}/topicmodel_${data:dataset}.pickle',
            'file_dictionary': '${models:directory}/topicdict_${data:dataset}.pickle'
        },
        'classification': {
            'train_model': False,
            'file_clf_tool': '${models:directory}/email_clf_tool.pickle'
        },
        'clustering': {
            'file_clustering_tool': '${models:directory}/clustering_tool.pickle'
        },
        'hierarchy_score_weights': {
            'degree': 0,
            'number_of_emails': 0.5,
            'clustering_values': 1,
            'hubs': 2,
            'authorities': 1,
            'response_score': 1,
            'average_time': 1,
            'mean_shortest_paths': 1,
            'number_of_cliques': 3,
            'raw_clique_score': 1,
            'weighted_clique_score': 1
        }
    }

    def __init__(self, override_args=None):
        """Init.

        TODO: write docstring
        :param override_args:
        """
        # logging.basicConfig(format='%(asctime)s.%(msecs)03d|%(name)s|%(levelname)s> %(message)s', datefmt='%H:%M:%S')
        # self.logger = logging.getLogger()

        conf_parser, self.conf_file = self._get_cli_conf_file(override_args)
        self.config = self._load_conf_file()

        self.args = self._get_cli_args(conf_parser, override_args)
        self.args = vars(self.args)

        # self.logger.setLevel(self.get('settings', 'log_level'))

        self._solr_url = None

        self._print_info()

    def get(self, section, option):
        """Get.

        TODO: write docstring
        :param section:
        :param option:
        :return:
        """
        arg = self.args.get(section + '_' + option, None)

        if arg is None:
            arg = self.config.get(section, option)

        if arg is None:
            raise KeyError

        if not isinstance(arg, str):
            return arg

        # try to convert stuff that is a string
        if arg == 'True':
            return True
        if arg == 'False':
            return False
        if arg == 'None':
            return None
        if arg.isdigit():
            return int(arg)
        try:
            return float(arg)
        except ValueError:
            pass

        # okay, probably was actually a string
        return arg

    @property
    def solr_url(self):
        """Solr URL.

        TODO: write docstring
        :return:
        """
        if self._solr_url:
            return self._solr_url
        self._solr_url = '{}://{}:{}/{}/'.format(self.get('solr', 'protocol'),
                                                 self.get('solr', 'host'),
                                                 self.get('solr', 'port'),
                                                 self.get('solr', 'url_path'))

        return self._solr_url

    def _print_info(self):
        """Print Info for local execution."""
        # TODO log with yarn logger
        # self.logger.info(self.args)

        # self.logger.info(('  Neo4j:\n'
        #                   '=========\n'
        #                   'Go to the neo4j directory and edit config/neo4j.conf\n'
        #                   '  dbms.directories.data={}\n'
        #                   '  dbms.directories.logs={}\n'
        #                   '  dbms.connectors.default_listen_address={}\n'
        #                   'Then start neo4j:'
        #                   '  $ ./bin/neo4j start\n'
        #                   '\n').format(self.get('neo4j', 'data_location'),
        #                                self.get('neo4j', 'log_location'),
        #                                self.get('neo4j', 'host')))

        # self.logger.info(('  Solr:\n'
        #                   '=========\n'
        #                   'Go to the solr directory run\n'
        #                   '  $ ./bin/solr start -h {} -s {} -p {}\n'
        #                   '\n').format(self.get('solr', 'host'),
        #                                self.get('solr', 'data_location'),
        #                                self.get('solr', 'port')))
        pass

    def _load_conf_file(self):
        """Docstring.

        TODO: write docstring
        :return:
        """
        config = ConfigParser(interpolation=ExtendedInterpolation())
        config.read_dict(self.DEFAULTS)
        config.read(self.conf_file)
        return config

    def _get_cli_conf_file(self, override_args):
        """Docstring.

        TODO: write docstring
        :param override_args:
        :return:
        """
        conf_parser = argparse.ArgumentParser(add_help=False)
        conf_parser.add_argument('-c', '--conf_file',
                                 help='Location of config file containing default settings',
                                 metavar='FILE',
                                 default=self.DEFAULT_CONFIG_FILE)
        args, _ = conf_parser.parse_known_args(override_args)
        return conf_parser, args.conf_file

    def _get_cli_args(self, conf_parser, override_args):
        """Docstring.

        Define CLI arguments here for things that might change more often than you would edit a
        config file.
        One might for example just turn on database imports in one run or have a different log level.

        :param conf_parser:
        :param override_args:
        :return:
        """
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
        parser.add_argument('--settings-run-distributed', action='store_false', default=None,
                            help='Set this to execute the pipeline in a distributed fashion')

        parser.add_argument('--data-source-dir',
                            help='Path to directory where raw emails are located.')
        parser.add_argument('--data-working-dir',
                            help='Path where results will be written to. WARNING: THIS DIRECTORY SHOULD NOT EXIST!')
        parser.add_argument('--data-time-min', type=int,
                            help='Start of time, everything beforehand will be pre-dated.')
        parser.add_argument('--data-time-max', type=int,
                            help='End of time, everything beforehand will be post-dated.')

        parser.add_argument('--solr-import', type=bool, default=None,
                            help='Set True if results should be written to solr.')
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

        parser.add_argument('--neo4j-import', type=bool, default=None,
                            help='Set True if results should be written to neo4j.')
        parser.add_argument('--neo4j-host',
                            help='hostname of neo4j instance')
        parser.add_argument('--neo4j-port',
                            help='port of neo4j instance')
        parser.add_argument('--neo4j-data-location',
                            help='neo4j data directory (used to start neo4j)')
        parser.add_argument('--neo4j-log-location',
                            help='neo4j log directory (used to start neo4j)')

        parser.add_argument('--spark-parallelism', type=int,
                            help='Spark parallelism')

        return parser.parse_args(override_args)
