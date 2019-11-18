import logging

from pagai.services.postgres import fetch_columns
from pagai.engine.models.ngram import NGramClassifier
from pagai.engine.models.rnn import RNNClassifier

# This is the training set, also used for testing and giving a score estimate
# works as follows (RESOURCE_TYPE, TABLE.COLUMN, NB_DATASETS)
# More specifically, the NB_DATASETS is the nb of item columns from training of
# a given length (ex: 100) you want to extract from this real column. The idea
# being that for a huge column (>1M) I get extract 100 representative columns
# sampled by replacement.

source = [
    # ("firstname", "firstnames.firstname", 100),
    # ("name", "names.name", 100),
    ("code", "patients.gender", 10),
    ("code", "admissions.marital_status", 10),
    ("code", "admissions.religion", 10),
    ("code", "admissions.insurance", 10),
    ("code", "admissions.admission_location", 10),
    ("code", "prescriptions.drug_type", 30),
    ("code", "prescriptions.dose_unit_rx", 20),
    ("date", "prescriptions.startdate", 90),
    ("date", "admissions.admittime", 10),
    ("id", "admissions.hadm_id", 10),
    ("id", "admissions.subject_id", 10),
    ("id", "prescriptions.subject_id", 80),
    # ("address", "addresses.road", 100),
    # ("city", "addresses.city", 100),
]


def build_training_set(connection):
    """Build training set."""
    datasets, labels = spec_from_source(source)

    columns = fetch_columns(datasets, dataset_size=100, connection=connection)

    return columns, labels


def train(columns, labels, model_type="ngram"):
    """
    Train a classification model on some dataset, and create a classification
    tool for columns of a given database, which will be used by the search
    engine. This database is intended to be different of the training database.
    :param owner: owner of the database
    :param database: database
    :param model: which model to use
    :return: the model train, with classification performed.
    """
    models = {"ngram": NGramClassifier, "rnn": RNNClassifier}
    model = models[model_type]()

    logging.warning("Preprocessing data...")
    X_train, y_train, X_test, y_test = model.preprocess(columns, labels)

    logging.warning("Fitting model...")
    model.fit(X_train, y_train)

    # Just to have an overview of the model performance on the training DB
    logging.warning("Score information:")
    y_pred = model.predict(X_test)
    model.score(y_pred, y_test)

    return model


def spec_from_source(source):
    """
    Tool function to convert the user-friendly format of the source description to
    a format compatiable with the model pipeline
    :param source:
    :return:
    """
    datasets = []
    labels = []
    for column in source:
        if len(column) >= 3:
            label, column_name, nb_datasets = column
        else:
            label, column_name, nb_datasets = column[0], column[1], 1
        datasets.append((column_name, nb_datasets))
        labels += [label.upper()] * nb_datasets

    return datasets, labels
