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
            'num_topics': 25,
            'alpha': 0.1,
            'beta': 0.1,
            'minimum_prediction_probability': 0.02,
            'file_model': '${models:directory}/topicmodel_${data:dataset}.pickle',
            'file_dictionary': '${models:directory}/topicdict_${data:dataset}.pickle',
            'file_corpus': '${models:directory}/topiccorpus_${data:dataset}.pickle'
        },
        'classification': {
            'train_model': False,
            'file_clf_tool': '${models:directory}/email_clf_tool.pickle'
        },
        'clustering': {
            'file_clustering_tool': '${models:directory}/clustering_tool.pickle'
        },
        'network_analysis': {
            'run': True
        },
        'correspondent_aggregation': {
            # Add domains that should be ignored in the organisation extraction here, please use lowercase.
            'false_organisations': [
                'yahoo',
                'aol',
                'gmail',
                'hotmail',
                'gmx',
                'web'
            ]
        },
        'phrase_detection': {
            'amount': 100,
            'window_width': 1000,
            'chunk_size': 10000,
            'length': (1, 2, 3, 4, 5, 6),
            'common_words': ['the','of','and','to','a','in','for','is','othe','of','and','to','a','in','for','is','on','that','by','this','with','i','you','it','not','or','be','are','from','at','as','your','all','have','new','more','an','was','we','will','home','can','us','about','if','page','my','has','search','free','but','our','one','other','do','no','information','time','they','site','he','up','may','what','which','their','news','out','use','any','there','see','only','so','his','when','contact','here','business','who','web','also','now','help','get','pm','view','online','c','e','first','am','been','would','how','were','me','s','services','some','these','click','its','like','service','x','than','find','price','date','back','top','people','had','list','name','just','over','state','year','day','into','email','two','health','n','world','re','next','used','go','b','work','last','most','products','music','buy','data','make','them','should','product','system','post','her','city','t','add','policy','number','such','please','available','copyright','support','message','after','best','software','then','jan','good','video','well','d','where','info','rights','public','books','high','school','through','m','each','links','she','review','years','order','very','privacy','book','items','company','r','read','group','sex','need','many','user','said','de','does','set','under','general','research','university','january','mail','full','map','reviews','program','life','know','games','way','days','management','p','part','could','great','united','hotel','real','f','item','international','center','ebay','must','store','travel','comments','made','development','report','off','member','details','line','terms','before','hotels','did','send','right','type','because','local','those','using','results','office','education','national','car','design','take','posted','internet','address','community','within','states','area','want','phone','dvd','shipping','reserved','subject','between','forum','family','l','long','based','w','code','show','o','even','black','check','special','prices','website','index','being','women','much','sign','file','link','open','today','technology','south','case','project','same','pages','uk','version','section','own','found','sports','house','related','security','both','g','county','american','photo','game','members','power','while','care','network','down','computer','systems','three','total','place','end','following','download','h','him','without','per','access','think','north','resources','current','posts','big','media','law','control','water','history','pictures','size','art','personal','since','including','guide','shop','directory','board','location','change','white','text','small','rating','rate','government','children','during','usa','return','students','v','shopping','account','times','sites','level','digital','profile','previous','form','events','love','old','john','main','call','hours','image','department','title','description','non','k','y','insurance','another','why','shall','property','class','cd','still','money','quality','every','listing','content','country','private','little','visit','save','tools','low','reply','customer','december','compare','movies','include','college','value','article','york','man','card','jobs','provide','j','food','source','author','different','press','u','learn','sale','around','print','course','job','canada','process','teen','room','stock','training','too','credit','point','join','science','men','categories','advanced','west','sales','look','english','left','team','estate','box','conditions','select','windows','photos','gay','thread','week','category','note','live','large','gallery','table','register','however','june','october','november','market','library','really','action','start','series','model','features','air','industry','plan','human','provided','tv','yes','required','second','hot','accessories','cost','movie','forums','march','la','september','better','say','questions','july','yahoo','going','medical','test','friend','come','dec','server','pc','study','application','cart','staff','articles','san','feedback','again','play','looking','issues','april','never','users','complete','street','topic','comment','financial','things','working','against','standard','tax','person','below','mobile','less','got','blog','party','payment','equipment','login','student','let','programs','offers','legal','above','recent','park','stores','side','act','problem','red','give','memory','performance','social','q','august','quote','language','story','sell','options','experience','rates','create','key','body','young','america','important','field','few','east','paper','single','ii','age','activities','club','example','girls','additional','password','z','latest','something','road','gift','question','changes','night','ca','hard','texas','oct','pay','four','poker','status','browse','issue','range','building','seller','court','february','always','result','audio','light','write','war','nov','offer','blue','groups','al','easy','given','files','event','release','analysis','request','fax','china','making','picture','needs','possible','might','professional','yet','month','major','star','areas','future','space','committee','hand','sun','cards','problems','london','washington','meeting','rss','become','interest','id','child','keep','enter','california','porn','share','similar','garden','schools','million','added','reference','companies','listed','baby','learning','energy','run','delivery','net','popular','term','film','stories','put','computers','journal','reports','co','try','welcome','central','images','president','notice','god','original','head','radio','until','cell','color','self','council','away','includes','track','australia','discussion','archive','once','others','entertainment','agreement','format','least','society','months','log','safety','friends','sure','faq','trade','edition','cars','messages','marketing','tell','further','updated','association','able','having','provides','david','fun','already','green','studies','close','common','drive','specific','several','gold','feb','living','sep','collection','called','short','arts','lot','ask','display','limited','powered','solutions','means','director','daily','beach','past','natural','whether','due','et','electronics','five','upon','period','planning','database','says','official','weather','mar','land','average','done','technical','window','france','pro','region','island','record','direct','microsoft','conference','environment','records','st','district','calendar','costs','style','url','front','statement','update','parts','aug','ever','downloads','early','miles','sound','resource','present','applications','either','ago','document','word','works','material','bill','apr','written','talk','federal','hosting','rules','final','adult','tickets','thing','centre','requirements','via','cheap','nude','kids','finance','true','minutes','else','mark','third','rock','gifts','europe','reading','topics','bad','individual','tips','plus','auto','cover','usually','edit','together','videos','percent','fast','function','fact','unit','getting','global','tech','meet','far','economic','en','player','projects','lyrics','often','subscribe','submit','germany','amount','watch','included','feel','though','bank','risk','thanks','everything','deals','various','words','linux','jul','production','commercial','james','weight','town','heart','advertising','received','choose','treatment','newsletter','archives','points','knowledge','magazine','error','camera','jun','girl','currently','construction','toys','registered','clear','golf','receive','domain','methods','chapter','makes','protection','policies','loan','wide','beauty','manager','india','position','taken','sort','listings','models','michael','known','half','cases','step','engineering','florida','simple','quick','none','wireless','license','paul','friday','lake','whole','annual','published','later','basic','sony','shows','corporate','google','church','method','purchase','customers','active','response','practice','hardware','figure','materials','fire','holiday','chat','enough','designed','along','among','death','writing','speed','html','countries','loss','face','brand','discount','higher','effects','created','remember','standards','oil','bit','yellow','political','increase','advertise','kingdom','base','near','environmental','thought','stuff']  # NOQA
        },
        'hierarchy_scores_weights': {
            'degree': 1,
            'number_of_emails': 1,
            'clustering_values': 1,
            'hubs': 1,
            'authorities': 1,
            'response_score': 1,
            'average_time': 1,
            'mean_shortest_paths': 1,
            'number_of_cliques': 1,
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
