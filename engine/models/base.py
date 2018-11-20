import logging

from engine.config import Config


class BaseClassifier:
    """
    The BaseClassifier is the Parent Class of all classifiers. It is use
    to factorize methods that we expect all classifiers to have.
    """
    def __init__(self):
        self.classification = None
        self.config = Config('model')

    def find_all(self, resource_type, max_col_len=20):
        if self.classification is None:
            raise TypeError('Model is not ready for live classification.')
        results = []
        for column in self.classification:
            if resource_type in column.proba_classes:
                column.score = column.proba_classes[resource_type]
            else:
                column.score = 1
                logging.warning('No ResourceType was provided')
            if column.score > self.config.min_score:
                column.data = column.data[:max_col_len]
                results.append(column)

        return results

