"""Classification of emails in the pipeline."""

import ujson as json
import pickle

from .common import Pipe
from .libs import email_clustering


class EmailClusterPrediction(Pipe):
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
        with open(self.conf.get('clustering', 'file_clustering_tool'), 'rb') as f:
            components = pickle.load(f)

            return email_clustering.EmailClusteringTool(
                components['knn_clf'],
                components['vectorizer_body'],
                components['vectorizer_subject'],
                components['insights']
            )

    def run_on_document(self, email_doc, clustering_tool):
        """Predict classes for a document."""
        document = json.loads(email_doc)

        prediction = clustering_tool.predict_cluster(document['raw'])
        document['cluster'] = str(prediction)

        return json.dumps(document, ensure_ascii=False)

    def run_on_partition(self, partition):
        """Load models partitionwise for performance reasons."""
        predictor = self.load_clf_tool()

        for elem in partition:
            yield self.run_on_document(elem, predictor)

    def run(self, rdd):
        """Run task in spark context."""
        return rdd.mapPartitions(lambda x: self.run_on_partition(x))
