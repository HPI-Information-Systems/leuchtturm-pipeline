"""
A small helper which writes all configured variables in a format, which can be sourced in bash.
All variables will be upper-cased, sections and options are separated by '_'

For example:
source <(python ini2bash.py -c sopedu-deploy-dnc.ini)

internally it just prints everything to stdout:
SETTINGS_LOG_LEVEL="INFO"
DATA_DATASET="DATASET"
DATA_SOURCE_DIR="./DATA/INPUT"
DATA_WORKING_DIR="./DATA/PROCESSED"
DATA_TIME_MIN="0"
DATA_TIME_MAX="2147483647"
SOLR_IMPORT="FALSE"
...
"""
from config import Config

conf = Config()
for section, options in conf.DEFAULTS.items():
    for option, value in options.items():
        print(section.upper() + '_' + option.upper() + '="' + str(conf.get(section, option)).upper() + '"')
print('SOLR_URL="' + conf.solr_url + '"')
