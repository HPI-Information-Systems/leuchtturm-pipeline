"""Python API for communication with Solr."""

import os
from urllib.request import urlopen


class SolrCommunicator():
    """
    A class for communication with Solr.

    Functionalities:
    * execute commands
    * add entries
    * request search terms
    """

    def escape_blanks(self, string):
        """Escape blanks for HTTP protocol."""
        return string.replace(' ', '%20')

    def execute_command(self, core, json_string, simulate=False):
        """
        Execute a json formatted string to a core.

        The json_string needs to have format like:
        {"add":{"doc":{"id":"anID","aField":"someContent"}}}
        Replace "add", "doc" and fields accordingly
        """
        print(simulate)
        command = ('curl http://localhost:8983/solr/' +
                   core +
                   '/update?commit=true -H "Content-Type: text/json" --data-binary \'' +
                   json_string + '\'')

        if not simulate:
            os.system(command)
        else:
            print('Pretend to upload with:')
        print(command)

    def execute_command_on_file(self, core, json_file_name, simulate=False):
        """
        Execute a json formatted string to a core.

        The json_string needs to have format like:
        {"add":{"doc":{"id":"anID","aField":"someContent"}}}
        Replace "add", "doc" and fields accordingly
        """
        # print(json_string)
        # json_string_replaced = json_string.replace("'", "\\\\'")
        # print(json_string_replaced)
        # command = ('curl http://localhost:8983/solr/' + core +
        #            '/update?commit=true -H "Content-Type: text/json" --data-binary \'' +
        # json_string + '\'')

        command = ('curl http://localhost:8983/solr/' +
                   core +
                   '/update?commit=true --data-binary @' +
                   json_file_name +
                   ' -H "Content-type:application/json"')

        if not simulate:
            os.system(command)
        else:
            print('Pretend to upload with:')
        print(command)

    def add_docs(self, core, documents, simulate=False):
        """
        Add documents in json format to a core.

        Provide documents as array of json-strings like:
        ['{"...":"..."}','{"...":"..."}',...]
        """
        if len(documents) == 0:
            return

        json_string = '{"add":{"doc":'

        for document in documents[:-1]:
            json_string += document + '},"add":{"doc":'

        json_string += documents[-1] + '}}'

        self.execute_command(core, json_string, simulate)

    def add_doc(self, core, document, simulate=False):
        """
        Add a document in json-string format to a core.

        Provide a document like: '{"aField":"someContent",...}'
        """
        json_string = '{"add":{"doc":' + document + '}}'

        self.execute_command(core, json_string, simulate)

    def add_doc_by_file(self, core, json_file_name, simulate=False):
        """
        Add a document in json-string format to a core.

        Provide a document like: '{"aField":"someContent",...}'
        """
        # json_string = '{"add":{"doc":' + document + '}}'

        self.execute_command_on_file(core, json_file_name, simulate)

    def request(self, core, search_term, simulate=False):
        """Request a search term to a core."""
        connection = urlopen('http://localhost:8983/solr/' +
                             core +
                             '/select?q=' +
                             self.escape_blanks(search_term) +
                             '&wt=python')

        if not simulate:
            response = eval(connection.read())
            print(response)
        else:
            print('Pretend to request term:')
            print(search_term)

    def request_distinct_doc_ids(self):
        """Request distinct doc_ids in email_body core."""
        connection = urlopen('http://localhost:8983/solr/email_bodies/select/?q=*%' +
                             '3A*&rows=0&facet=on&facet.field=doc_id&wt=json')

        return eval(connection.read())['facet_counts']['facet_fields']['doc_id'][::2]
