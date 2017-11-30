"""This module is responsible for email cleansing."""

import luigi
from spark_pipeline import EmailBodyExtractor
# import findspark
# findspark.init()
from pyspark import SparkContext
from datetime import datetime
import json
import talon
from talon import quotations
from talon.signature.bruteforce import extract_signature
talon.init()


DATETIMESTAMP = datetime.now().strftime('%Y-%m-%d_%H:%M')


class EmailCleaner(luigi.Task):
    """This task uses Talon from Mailgun to clean emails."""

    def requires(self):
        """Dependency: FileLister like file with field: body."""
        return EmailBodyExtractor()

    def output(self):
        """Write a HDFS target with timestamp."""
        return luigi.contrib.hdfs.HdfsTarget('/pipeline/emails_cleaned/' + DATETIMESTAMP + '_emails_cleaned.txt')

    def run(self):
        """Run luigi task."""
        sc = SparkContext()
        data = open(self.input().path).read().splitlines()
        myRdd = sc.parallelize(data)
        results = myRdd.map(lambda x: self.clean_entry(x)).collect()
        with self.output().open('w') as f:
            for result in results:
                f.write(result + '\n')
        sc.stop()

    def clean_entry(self, entry):
        """Read body, and clean it of noise using Talon."""
        dict = json.loads(entry)
        body = dict['full_body'].replace('\\n', '\n')
        reply = quotations.extract_from_plain(body)
        text, signat = extract_signature(reply)
        dict['reply'] = reply
        dict['reply_text'] = text
        # dict['signature'] = signat

        return json.dumps(dict, ensure_ascii=False)
