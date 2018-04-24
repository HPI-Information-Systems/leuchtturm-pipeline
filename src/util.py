"""Utils."""

import configparser
from pathlib import PurePath


def get_config(dataset):
    """Get a config and parse it."""
    config_file = 'config-' + dataset + '.ini'
    config = configparser.ConfigParser()
    config.read('./config/' + config_file)

    return config
