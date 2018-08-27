import pysolr
import json
import argparse
from tqdm import tqdm

parser = argparse.ArgumentParser(description='Download all documents from a solr core')
parser.add_argument('HOST', help='Host of solr server, e.g. 172.16.64.23 for isfet')
parser.add_argument('PORT', help='Port of solr server, e.g. 8983 for default port')
parser.add_argument('CORE', help='Core to write to on server')
parser.add_argument('SOURCE', help='target file to read from (one line = one doc as json)')
parser.add_argument('--rows', help='number of rows per request', default=100)
args = parser.parse_args()

target_dir = ''
solr_url = args.HOST
solr_port = args.PORT
solr_core = args.CORE

client = pysolr.Solr('http://{}:{}/solr/{}'.format(solr_url, solr_port, solr_core))
num_docs = sum(1 for line in open(args.SOURCE))

with tqdm(total=num_docs) as pbar, open(args.SOURCE, 'r') as f:
    buffer = []
    for line in f:
        doc = json.loads(line)
        del doc['_version_']
        buffer.append(doc)
        if len(buffer) >= args.rows:
            client.add(buffer)
            pbar.update(len(buffer))
            buffer = []
    client.add(buffer)
    pbar.update(len(buffer))
    
# python uploadSolr.py 172.16.64.23 8983 enron enron.json
# python uploadSolr.py 172.16.64.23 8983 enron_topics enron_topics.json
# python uploadSolr.py 172.16.64.23 8983 enron_dev enron_dev.json
# python uploadSolr.py 172.16.64.23 8983 enron_dev_topics enron_dev_topics.json
# python uploadSolr.py 172.16.64.23 8983 dnc dnc.json
# python uploadSolr.py 172.16.64.23 8983 dnc_topics dnc_topics.json
