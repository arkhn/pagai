import jellyfish


class Column:
    def __init__(self, table, column, data=None, score=0):
        self.table = table
        self.column = column
        self.data = data
        self.score = score
        self.proba_classes = None

    def set_proba_classes(self, proba_classes):
        self.proba_classes = proba_classes

    def match_name_score(self, query):
        corpus = [self.table, self.column] + self.column.split(".")
        score = 10e10
        for word in corpus:
            distance = jellyfish.damerau_levenshtein_distance(word, query)
            if query in word:
                distance /= 2
            score = min(score, distance)
        return score

    def serialize(self):
        """
        Transform Column in a dict for readability

        Example:
            Column(table, column, data, score)
            ->
            {
                'table':    'patient',
                'column':   'patient.col1'
            }
        """
        data_dct = {
            "table": self.table,
            "column": self.column,
            "data": self.data,
            "score": self.score,
        }
        return data_dct
