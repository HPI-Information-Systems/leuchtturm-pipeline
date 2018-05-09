"""Classification of emails in the pipeline."""

import ujson as json
from random import uniform
from operator import itemgetter
from .common import Pipe


class EmailCategoryClassification(Pipe):
    """Classify emails in categories (work, personal, spam).

    Classify emails based on two (or three) different given models.
    Assign classes to every email doc.
    """

    def __init__(self, conf):
        """Initialization."""
        super().__init__(conf)
        self.conf = conf
        self.fake_categories = ['business', 'personal', 'spam']
        self.fake_subcategories = ['scheduling', 'strategy', 'family', 'sports']

    def load_model(self):
        """Load model from path."""
        file_model = self.conf.get('classification', 'file_model')
        return file_model

    def get_category_for_document(self, document):
        """Predict classes for an email document, enable fake classes."""
        def get_fake_categories():
            prob_category = dict()
            a = uniform(0, 1)
            b = uniform(0, 1 - a)
            c = 1 - a - b
            for category, value in zip(self.fake_categories, [a, b, c]):
                prob_category[category] = value

            return prob_category

        def get_fake_subcategories():
            prob_subcategory = []
            for subcategory in self.fake_subcategories:
                prob_subcategory.append(
                    {'name': subcategory, 'prob': uniform(0, 1)}
                )

            return prob_subcategory

        def find_max(prob_category):
            return max(prob_category.items(), key=itemgetter(1))[0]

        def find_max_sub(prob_subcategory):
            maximum = ''
            current = 0
            for dic in prob_subcategory:
                if dic['prob'] > current:
                    maximum = dic['name']
                    current = dic['prob']

            return maximum

        category = dict()
        model = self.load_model()
        if not model:
            prob_category = get_fake_categories()
            prob_subcategory = get_fake_subcategories()
        else:
            prob_category = {}  # TODO get categories with model
            prob_subcategory = {}  # TODO get subcategories with model

        category['top_category'] = find_max(prob_category)
        category['prob_category'] = prob_category

        category['top_subcategory'] = find_max_sub(prob_subcategory)
        category['prob_subcategory'] = prob_subcategory

        return category

    def run_on_document(self, email_doc, model):
        """Predict classes for a document."""
        document = json.loads(email_doc)

        category = self.get_category_for_document(document, model)
        document['category'] = category

        return json.dumps(document)

    def run_on_partition(self, partition):
        """Run task in spark context. Partitionwise for performance reasons."""
        model = self.load_model()

        for doc in partition:
            yield self.run_on_document(doc, model)

    def run(self, rdd):
        """Run task in spark context."""
        return rdd.mapPartitions(lambda x: self.run_on_partition(x))
