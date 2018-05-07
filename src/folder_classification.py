"""Classification of emails in the pipeline."""

import ujson as json
from random import uniform
from .common import Pipe


class EmailFolderClassification(Pipe):
    """Classify emails based on folders (personal, social networks, advertisement, etc.).

    Classify emails based on a trained model.
    Assign classes to every email doc.
    """

    def __init__(self, conf):
        """Initialization."""
        super().__init__(conf)
        self.fake_folders = ['personal', 'social_networks', 'advertisement', 'spam']

    def load_model(self):
        """Load model from path."""
        raise NotImplementedError

    def get_folder_for_document(self, document, model=None):
        """Predict classes for an email document, enable fake classes."""
        def get_fake_folders():
            prob_folder = []
            for folder in self.fake_folders:
                prob_folder.append({'name': folder, 'prob': uniform(0, 1)})

            return prob_folder

        def find_max(prob_folder):
            maximum = ''
            current = 0
            for dic in prob_folder:
                if dic['prob'] > current:
                    maximum = dic['name']
                    current = dic['prob']

            return maximum

        folder = dict()
        if not model:
            prob_folder = get_fake_folders()
        else:
            prob_folder = {}  # TODO get subcategories with model

        folder['top_folder'] = find_max(prob_folder)
        folder['prob_folder'] = prob_folder

        return folder

    def run_on_document(self, email_doc, model=None):
        """Predict classes for a document."""
        document = json.loads(email_doc)

        folder = self.get_folder_for_document(document, model)
        document['folder'] = folder

        return json.dumps(document)

    def run_on_partition(self, partition):
        """Run task in spark context. Partitionwise for performance reasosn."""
        model = None  # self.load_model()

        for doc in partition:
            yield self.run_on_document(doc, model=model)

    def run(self, rdd):
        """Run task in spark context."""
        return rdd.mapPartitions(lambda x: self.run_on_partition(x))
