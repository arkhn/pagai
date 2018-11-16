class Column:
    def __init__(self, table, column, data=None, score=0):
        self.table = table
        self.column = column
        self.data = data
        self.score = score
        self.proba_classes = None

    def set_proba_classes(self, proba_classes):
        self.proba_classes = proba_classes

    def ser(self):
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
            'table': self.table,
            'column': self.column,
            'data': self.data,
            'score': self.score
        }
        return data_dct
