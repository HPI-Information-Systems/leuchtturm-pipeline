"""Utils. Needed for tests."""

import configparser


def get_config(dataset):
    """Get a config and parse it."""
    config = configparser.ConfigParser()

    if not dataset:
        return config  # return empty conf if not specified

    config.read('./config/config-' + dataset + '.ini')

    return config
