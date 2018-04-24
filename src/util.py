"""Utils."""

import configparser


def get_config(dataset):
    """Get a config and parse it."""
    if not dataset:
        return configparser.ConfigParser()  # return empty conf if not specified

    config_file = 'config-' + dataset + '.ini'
    config = configparser.ConfigParser()
    config.read('./config/' + config_file)

    return config
