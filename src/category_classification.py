"""Classification of emails in the pipeline."""

import ujson as json
import pickle

from .common import Pipe
from .libs import email_classification


class EmailCategoryClassification(Pipe):
    """Classify emails in categories (work, personal, spam).

    Classify emails based on two (or three) different given models.
    Assign classes to every email doc.
    """

    def __init__(self, conf):
        """Initialization."""
        super().__init__(conf)
        self.conf = conf

    def load_clf_tool(self):
        """Load classifier and required vectorizers."""
        with open(self.conf.get('classification', 'file_clf_tool'), 'rb') as f:
            components = pickle.load(f)

        return email_classification.EmailClassificationTool(
            components['classifier_3'],
            components['classifier_genre'],
            components['vectorizer_body'],
            components['vectorizer_subject'],
            components['labels_3'],
            components['labels_genre']
        )

    def run_on_document(self, email_doc, email_clf_tool):
        """Predict classes for a document."""
        document = json.loads(email_doc)

        # prediction = {'top_category': email_clf_tool.predict_top_category(document['raw']),
        #               'prob_category': email_clf_tool.predict_proba(document['raw'])}
        document['category'] = email_clf_tool.predict(document['raw'])

        return json.dumps(document, ensure_ascii=False)

    def run_on_partition(self, partition):
        """Load models partitionwise for performance reasons."""
        predictor = self.load_clf_tool()

        for elem in partition:
            yield self.run_on_document(elem, predictor)

    def run(self, rdd):
        """Run task in spark context."""
        return rdd.mapPartitions(lambda x: self.run_on_partition(x))
