from config import Config

conf = Config()
for section, options in conf.DEFAULTS.items():
    for option, value in options.items():
        print(section.upper() + '_' + option.upper() + '="' + str(conf.get(section, option)).upper() + '"')
print('SOLR_URL="' + conf.solr_url + '"')
