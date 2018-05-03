"""Classification of emails in the pipeline."""

import ujson as json

from .common import Pipe
from .libs.clf import email_clf


class EmailCategoryClassification(Pipe):
    """Classify emails in categories (work, personal, spam).

    Classify emails based on two (or three) different given models.
    Assign classes to every email doc.
    """

    def __init__(self):
        """Initialization."""
        super().__init__()

    def run_on_document(self, email_doc):
        """Predict classes for a document."""
        document = json.loads(email_doc)

        prediction = {'top_category': email_clf.get_email_category(document['raw']),
                      'prob_category': email_clf.get_category_prob(document['raw'])}
        document['category'] = prediction

        return json.dumps(document)

    def run(self, rdd):
        """Run task in spark context."""
        return rdd.map(lambda x: self.run_on_document(x))
