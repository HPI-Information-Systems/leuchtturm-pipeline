"""This module cleans and filters non-email documents."""

import luigi

from spark_pipeline import FileLister
from pyspark import SparkContext
from datetime import datetime
import json

DATETIMESTAMP = datetime.now().strftime('%Y-%m-%d_%H-%M')


class DocumentCleaner(luigi.Task):
    """Filter out non-emails, useless metadata files and cluttered content."""

    dump_path = luigi.Parameter(default='./luigi_dumps/document_cleaner/')

    def requires(self):
        """Raw data."""
        return FileLister()

    def output(self):
        """Add metadata to each mail."""
        return luigi.LocalTarget(self.dump_path +
                                 DATETIMESTAMP +
                                 '_cleandocuments.txt')

    def run(self):
        """Perform the task's action."""
        sc = SparkContext()
        # data = sc.textFile(self.input().path)
        data = open(self.input().path).read().splitlines()
        myRdd = sc.parallelize(data)
        result = myRdd.map(lambda x: self.clean_documents(x)).collect()
        with open(self.output().path, 'w', encoding='utf8') as outfile:
            for mail in result:
                if mail != "{}":
                    outfile.write("%s\n" % mail)
        sc.stop()

    def clean_documents(self, document_json):
        """Isolate removal of unnecessary filels and clutter."""
        document = json.loads(document_json)

        clean_body = document["full_body"]

        # document is email and should not enter the document pipeline
        if document["full_body"].lower().startswith("subject"):
            return json.dumps({}, ensure_ascii=False)
        # document is weird meta data and not interesting
        elif document["full_body"].lower().startswith("name"):
            return json.dumps({}, ensure_ascii=False)
        else:
            special_chars = ['"', "!", "#", "$", "%", "&", "'", "§", "(", ")", "*", "+",
                             "-", ".", "/", ":", ";", "<", "=", ">", "?",
                             "@", "[", "\\", "]", "^", "_", "`", "{", "|", "}", "~", "\n", "\u000b", "\f"]

            for char in special_chars:
                clean_body = clean_body.replace(char, "")

        document["full_body"] = clean_body.lower()

        return json.dumps(document, ensure_ascii=False)
