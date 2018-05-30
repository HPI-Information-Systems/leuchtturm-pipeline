"""A module to upload buffered results quickly for debug purposes."""
import json
import py2neo


files = ['hierarchy', 'community', 'role']
working_file = files[0]


neo4j_host = 'http://172.16.64.28'  # 'http://sopedu.hpi.uni-potsdam.de',
http_port = 61100
bolt_port = 61000


def update_network(labelled_nodes, attribute):
    """Update neo4j's data with the detected labels."""
    if labelled_nodes:
        print('- start upload of ' + attribute + ' labels.')
        neo_connection = py2neo.Graph(neo4j_host, http_port=http_port, bolt_port=bolt_port)
        neo_connection.run('UNWIND $labelled_nodes AS ln '
                           'MATCH (node) WHERE ID(node) = ln.node_id '
                           'SET node.' + attribute + ' = ln.' + attribute,
                           labelled_nodes=labelled_nodes, attribute=attribute)
        print('- finished upload of ' + attribute + ' labels.')


with open(working_file + '.json') as f:
    data = json.load(f)

update_network(data, working_file)
