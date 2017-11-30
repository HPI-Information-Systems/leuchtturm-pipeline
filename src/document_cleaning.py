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
        """Excecute meta data extraction."""
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

    def clean_documents(self, input):
        """Extract meta data of each email."""
        dict = json.loads(input)

        # document is email and should not enter the document pipeline
        if dict["full_body"].lower().startswith("subject"):
            return json.dumps({}, ensure_ascii=False)
        # document is weird meta data and not interesting
        elif dict["full_body"].lower().startswith("name"):
            return json.dumps({}, ensure_ascii=False)
        else:
            return json.dumps(dict, ensure_ascii=False)
