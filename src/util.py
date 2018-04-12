"""Utils."""
import configparser
from pathlib import PurePath


def get_config(dataset):
    """Get a config and parse it."""
    config_file = 'config-' + dataset + '.ini'
    configpath = PurePath('.') / 'config' / config_file
    config = configparser.ConfigParser()
    config.read(str(configpath))
    return config
