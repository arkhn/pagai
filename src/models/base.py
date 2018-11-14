from src import loader
import operator


class BaseClassifier:
    def __init__(self):
        self.classification = None

    def find_all(self, resource_type, limit=20):
        if self.classification is None:
            raise TypeError('Model is not ready for live classification.')
        results = []
        for column_name, column in self.classification.items():
            proba = column['labels'][resource_type]
            if proba > 0.1:
                results.append([proba, column_name, column])

        results.sort(key=operator.itemgetter(0), reverse=True)

        return results[:limit]
