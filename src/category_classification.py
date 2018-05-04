"""Classification of emails in the pipeline."""

import ujson as json
import pickle

from .common import Pipe
from .libs import clf_export


class EmailCategoryClassification(Pipe):
    """Classify emails in categories (work, personal, spam).

    Classify emails based on two (or three) different given models.
    Assign classes to every email doc.
    """

    def __init__(self):
        """Initialization."""
        super().__init__()

    def load_clf(self):
        """Load classifier and required vectorizers."""
        with open('./models/email_clf.pickle', 'rb') as f:
            clf = pickle.load(f)

        with open('./models/email_vectorizer_body.pickle', 'rb') as f:
            vectorizer_body = pickle.load(f)

        with open('./models/email_vectorizer_subject.pickle', 'rb') as f:
            vectorizer_subject = pickle.load(f)

        with open('./models/email_encoder.pickle', 'rb') as f:
            label_encoder = pickle.load(f)

        return clf_export.EmailClfInterface(clf, vectorizer_body, vectorizer_subject, label_encoder)

    def run_on_document(self, email_doc, email_clf):
        """Predict classes for a document."""
        document = json.loads(email_doc)

        prediction = {'top_category': email_clf.predict_top_category(document['raw']),
                      'prob_category': email_clf.predict_proba(document['raw'])}
        document['category'] = prediction

        return json.dumps(document)

    def run_on_partition(self, partition):
        """Load models partitionwise for performance reasons."""
        predictor = self.load_clf()

        for elem in partition:
            yield self.run_on_document(elem, predictor)

    def run(self, rdd):
        """Run task in spark context."""
        return rdd.mapPartitions(lambda x: self.run_on_partition(x))
