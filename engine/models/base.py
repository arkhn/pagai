from engine import loader
import operator


class BaseClassifier:
    """
    The BaseClassifier is the Parent Class of all classifiers. It is use
    to factorize methods that we expect all classifiers to have.
    """
    def __init__(self):
        self.classification = None

    def find_all(self, resource_type, max_col_len=20):
        if self.classification is None:
            raise TypeError('Model is not ready for live classification.')
        results = []
        for column in self.classification:
            column.score = column.proba_classes[resource_type]
            if column.score > 0.1:
                column.data = column.data[:max_col_len]
                results.append(column)

        return results

