from src import loader


class BaseClassifier:
    def __init__(self):
        self.classification = None

    def find_all(self, resource_type):
        if self.classification is None:
            raise TypeError('Model si not ready for live classification.')
        columns = self.classification[resource_type]
        return columns