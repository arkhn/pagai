from engine import loader
import operator


class BaseClassifier:
    """
    The BaseClassifier is the Parent Class of all classifiers. It is use
    to factorize methods that we expect all classifiers to have.
    """
    def __init__(self):
        self.classification = None

    def find_all(self, resource_type, max_res=10, max_col_len=20):
        if self.classification is None:
            raise TypeError('Model is not ready for live classification.')
        results = []
        for column_name, column in self.classification.items():
            proba = column['labels'][resource_type]
            if proba > 0.1:
                data = column['data'][:max_col_len]
                results.append([proba, column_name, data])

        results.sort(key=operator.itemgetter(0), reverse=True)

        truncated_results = results[:max_res]

        return self.api_response(truncated_results, ('score', 'column_name', 'data'))

    @staticmethod
    def api_response(results, format_names):
        """
        Transform tuple-like response in a dict to readability

        Example:

            [(0.1, 'table.col1'), ... ], ('score', 'column')
            ->
            {
                'score':    0.1,
                'column':   'table.col1'
            }
        """
        response = []
        for res in results:
            item = {
                name: el
                for name, el in zip(format_names, res)
            }
            response.append(item)

        return response
