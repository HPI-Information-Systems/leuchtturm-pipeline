"""
This module deals with E-Mail Preprocessing.

All jobs for datacleasnig and first meta data extraction such as header, body are included here.
"""

import luigi
import json
import os
import datetime
from solr_communicator import SolrCommunicator
from langdetect import detect
import emailbody.extractmailbody as body_extractor

DATETIME = datetime.datetime.now().strftime("%y-%m-%d_%H-%M")


def build_local_target(dump_path, email_path):
    """Given file path and email path, this function returns a LocalTarget for Luigi's output."""
    return luigi.LocalTarget(dump_path +
                             DATETIME +
                             '_' +
                             os.path.basename(email_path).replace('.txt', '.json'))


class EmailBodyExtractor(luigi.Task):
    """
    Given a filepath, this task extracts the body of an e-mail and writes it to a file at the target path.

    This job uses Quagga by Tim Repke.
    """

    email_path = luigi.Parameter(default="mails/mail.txt")
    dump_path = luigi.Parameter(default='./luigi_dumps/email_body_extractor/')

    def output(self):
        """Produce file at given targetpath."""
        return build_local_target(self.dump_path, self.email_path)

    def run(self):
        """Run body extraction job."""
        doc = {"doc_id": os.path.basename(self.email_path).replace('.txt', ''),
               "email_full_body": body_extractor.getBody(self.email_path)}
        with self.output().open('w') as f:
            f.write(json.dumps([doc]))


class EnronFooterRemover(luigi.Task):
    """Given a filepath, this task removes the footer that was added after the initial export of the data set."""

    email_path = luigi.Parameter(default="mails/mail.txt")
    dump_path = luigi.Parameter(default='./luigi_dumps/enron_footer_remover/')
    footer = ('***********EDRM Enron Email Data Set has been produced in EML, PST and NSF format by ZL Technologies, '
              'Inc. This Data Set is licensed under a Creative Commons Attribution 3.0 United States License '
              '<http://creativecommons.org/licenses/by/3.0/us/> . To provide attribution, please cite to "ZL '
              'Technologies, Inc. (http://www.zlti.com)."***********')

    def requires(self):
        """Require e-mail text without headers."""
        return EmailBodyExtractor(self.email_path)

    def output(self):
        """Override file at given path without footer."""
        return build_local_target(self.dump_path, self.email_path)

    def run(self):
        """Replace footer with empty string."""
        body_text = ''
        with self.input().open('r') as f:
            body_text = json.loads(f.read())[0]['email_full_body'].replace(self.footer, '')

        doc = {"doc_id": os.path.basename(self.email_path).replace('.txt', ''),
               "email_body": body_text}
        with self.output().open('w') as f:
            f.write(json.dumps([doc]))


class LanguageDetector(luigi.Task):
    """
    Given a filepath, this task extracts the body of an e-mail and writes it to a file at the target path.

    This job uses Quagga by Tim Repke.
    """
    email_path = luigi.Parameter(default="mails/mail.txt")
    dump_path = luigi.Parameter(default='./luigi_dumps/language_detection/')

    def requires(self):
        """Require e-mail text without headers."""
        return EnronFooterRemover(self.email_path)

    def run(self):
        """Replace footer with empty string."""
        body_text = ''
        with self.input().open('r') as f:
            email_body = json.loads(f.read())[0]['email_body']
            language = detect(email_body)

        doc = {"doc_id": os.path.basename(self.email_path).replace('.txt', ''),
               "email_body": body_text,
               "language": language}
        with self.output().open('w') as f:
            f.write(json.dumps([doc]))

    def output(self):
        """Override file at given path without footer."""
        return build_local_target(self.dump_path, self.email_path)


class LoadEmailToSolr(luigi.Task):
    """Given a filepath, this task writes the files content to a solr core."""

    email_path = luigi.Parameter(default='./mails/0000000_0001_000000029.txt')
    solr_core = luigi.Parameter(default='email_bodies')
    dump_path = './luigi_dumps/solr/'

    def requires(self):
        """Require e-mail text without headers and enron footer."""
        return {"enron_footer_remover": EnronFooterRemover(email_path=self.email_path),
                "email_body_extractor": EmailBodyExtractor(email_path=self.email_path)}

    def output(self):
        """Override file at given path without footer."""
        return build_local_target(self.dump_path, self.email_path)

    def run(self):
        """Read content of file and write core with id (filepath) to solr."""
        body = ''
        with self.input()['enron_footer_remover'].open('r') as f:
            body = json.loads(f.read())[0]['email_body']
        full_body = ''
        with self.input()['email_body_extractor'].open('r') as f:
            full_body = json.loads(f.read())[0]['email_full_body']

        doc = {"doc_id": os.path.basename(self.email_path).replace('.txt', ''),
               "email_body": body,
               "email_full_body": full_body}

        with self.output().open('w') as f:
            f.write(json.dumps([doc]))
        temporary_path = self.dump_path + DATETIME + '_' + os.path.basename(self.email_path).replace('.txt', '.json')
        SolrCommunicator().add_doc_by_file(self.solr_core, temporary_path)


class EMailPreprocesser(luigi.WrapperTask):
    """
    Wrappertask to perform all preprocessing steps.

    Given a source directory, this dummy-task performs the tasks EmailBodyExtractor, EnronFooterRemover, and
    LoadEmailToSolr on all files found in this direcectory.
    """

    source_directory = luigi.Parameter(default='./mails/')

    def requires(self):
        """Initialize one task per file for body extraction."""
        dependencies = []
        for file in os.listdir(self.source_directory):
            if not file.startswith("."):
                dependencies.append(LoadEmailToSolr(email_path=self.source_directory + file))
        return dependencies
