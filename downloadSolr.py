import requests
import json
import argparse
from tqdm import tqdm

parser = argparse.ArgumentParser(description='Download all documents from a solr core')
parser.add_argument('HOST', help='Host of solr server, e.g. 172.16.64.12 for sopedu')
parser.add_argument('PORT', help='Port of solr server, e.g. 8983 for default port')
parser.add_argument('CORE', help='Core to fetch from server')
parser.add_argument('TARGET', help='target file to write to (one line = one doc as json)')
parser.add_argument('--rows', help='number of rows per request', default=100)
args = parser.parse_args()

target_dir = ''
solr_url = args.HOST
solr_port = args.PORT
solr_core = args.CORE

r = requests.get('http://{}:{}/solr/{}/select?q=*%3A*&rows=1&sort=id+asc&wt=json&cursorMark=*'.format(
    solr_url, solr_port, solr_core))
res = r.json()
num_results = res['response']['numFound']

with tqdm(total=num_results) as pbar, open(args.TARGET, 'w') as f:
    cursor_mark = '*'
    while True:
        r = requests.get('http://{}:{}/solr/{}/select?q=*%3A*&rows={}&sort=id+asc&wt=json&cursorMark={}'.format(
            solr_url, solr_port, solr_core, args.rows, cursor_mark))
        res = r.json()

        for doc in res['response']['docs']:
            f.write(json.dumps(doc) + '\n')

        # print('Got {} docs | {}/{}  ({:.2f}%)'.format(len(res['response']['docs']),
        #                                              cnt, res['response']['numFound'],
        #                                              cnt / res['response']['numFound']))
        pbar.update(len(res['response']['docs']))
        cursor_mark = res['nextCursorMark']

# python download.py 172.16.64.12 8983 enron enron.json
# python download.py 172.16.64.12 8983 enron_topics enron_topics.json
# python download.py 172.16.64.12 8983 enron_dev enron_dev.json
# python download.py 172.16.64.12 8983 enron_dev_topics enron_dev_topics.json
# python download.py 172.16.64.12 8983 dnc dnc.json
# python download.py 172.16.64.12 8983 dnc_topics dnc_topics.json
