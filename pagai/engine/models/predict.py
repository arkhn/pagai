from pagai.engine.structure import Column


def classify(model, columns, labels=None):
    """Gets model, columns,
    predicts class probabilities for each column
    and return them."""

    X, columns = model.preprocess(columns, labels=labels, test_only=True)
    # Keep prediction probabilities,
    # it will be used for scoring
    y_pred = model.predict_proba(X)

    classification = []
    labels = [model.pred2label(input) for input in model.classes]

    # Classify columns
    for column, pred_proba in zip(columns, y_pred):
        column_name, column_data = column
        table_name = column_name.split(".")[0]
        column = Column(table_name, column_name, data=column_data)
        proba_classes = {l: p for l, p in zip(labels, pred_proba)}
        column.set_proba_classes(proba_classes)
        classification.append(column)

    return classification
