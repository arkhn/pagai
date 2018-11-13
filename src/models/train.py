import logging
import psycopg2

from sklearn.model_selection import cross_val_score

from src import loader
from src.models import ngram

source = [
    ('firstname', 'firstnames.firstname', 100),

    ('name', 'names.name', 100),

    ('code', 'patients.gender', 10),
    ('code', 'admissions.marital_status', 10),
    ('code', 'admissions.religion', 10),
    ('code', 'admissions.insurance', 10),
    ('code', 'admissions.admission_location', 10),
    ('code', 'prescriptions.drug_type', 30),
    ('code', 'prescriptions.dose_unit_rx', 20),

    ('date', 'prescriptions.startdate', 90),
    ('date', 'admissions.admittime', 10),

    ('id', 'admissions.hadm_id', 10),
    ('id', 'admissions.subject_id', 10),
    ('id', 'prescriptions.subject_id', 80),

    ('address', 'addresses.road', 100),

    ('city', 'addresses.city', 100)
]


def train(owner, database, model='ngram'):
    assert model == 'ngram'

    datasets, labels = spec_from_source(source)

    logging.warning('Fetching data...')
    columns = loader.fetch_columns(datasets, dataset_size=100)

    model = ngram.NGramClassifier()
    logging.warning('Preprocessing data...')
    X_train, y_train, X_test, y_test = model.preprocess(columns, labels)

    # X_train, y_train = clf.n_gram_fit_transform(X_train, y_train, ngram_range=(2, 3))
    # scores = cross_val_score(clf.clf, X, y, cv=5)
    # scores.mean()

    logging.warning('Fitting model...')
    model.fit(X_train, y_train, ngram_range=(2, 3))

    # print(clf.clf.feature_importances_)

    logging.warning('Score information:')
    y_pred = model.predict(X_test)
    model.score(y_pred, y_test)

    logging.warning('Building classification...')
    sql_params = loader.sql_params
    # sql_params['database'] = database
    with psycopg2.connect(**sql_params) as connection:
        prod_tables = loader.get_tables(connection)
        prod_table_columns = [
            '{}.{}'.format(table, column)
            for table in prod_tables
            for column in loader.get_columns(table, connection)
        ]
        prod_source = [
            ('unknown', table_column, 1) for table_column in prod_table_columns
        ]
        prod_datasets, _labels = spec_from_source(prod_source)
        columns = loader.fetch_columns(prod_datasets, dataset_size=100)
        X, columns = model.preprocess(columns, _labels, test_only=True)
        y_pred = model.predict(X)

        classification = {}
        for column, pred_key in zip(columns, y_pred):
            column_name, column_data = column
            label = model.pred2label(pred_key)
            if label not in classification:
                classification[label] = []
            full_column_name = '{}.{}'.format(owner, column_name)
            column = {
                'name': full_column_name,
                'data': column_data
            }
            classification[label].append(column)
        model.classification = classification

    return model


def spec_from_source(source):
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
