import re
import random

from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer

from .utils import *


class NGramClassifier:
    def __init__(self, n_gram_size=3):
        self.n_gram_size = n_gram_size
        self.clf = RandomForestClassifier(n_estimators=300, max_depth=4, random_state=0)

    def preprocess(self, columns, labels):
        """
        Reorganise data from the SQL loader, add stats features and split in train/test
        """
        self.labels = labels
        self.n_labels = len(self.labels)

        X_train, y_train, X_test, y_test = self.build_datasets(columns, labels)

        return X_train, y_train, X_test, y_test

    def fit(self, X_train, y_train, ngram_range=(2, 4)):
        X_train, y_train = self.n_gram_fit_transform(X_train, y_train, ngram_range)
        self.clf.fit(X_train, y_train)

    def predict(self, X_test):
        X_test = self.n_gram_transform(X_test)
        y_pred = self.clf.predict(X_test)
        return y_pred

    def build_datasets(self, columns, labels):
        """
        Add stat features about the column dataset and split into train/test
        """
        X, y = [], []
        for column, label in zip(columns, labels):
            column_name, dataset = column
            ilabel = [i for i, l in enumerate(self.labels) if l == label][0]
            stat_features = self.add_stat_features(dataset)
            X.append([column_name, stat_features, dataset])
            y.append(ilabel)

        X_train, y_train = [], []
        X_test, y_test = [], []
        n_items = len(X)
        ix = list(range(n_items))
        random.shuffle(ix)
        for i in range(n_items):
            if i < 0.8 * n_items:
                X_train.append(X[ix[i]])
                y_train.append(y[ix[i]])
            else:
                X_test.append(X[ix[i]])
                y_test.append(y[ix[i]])

        return X_train, y_train, X_test, y_test

    @staticmethod
    def add_stat_features(dataset):
        """
        Add several stats features about the dataset of each column
        """
        # 1. len of the row
        n_char = np.vectorize(len)(dataset).mean()
        # 2. Nb of words
        n_words = np.vectorize(lambda x: len(x.split(' ')))(dataset).mean()
        # 3. Percentage of unique values / all values
        p_unique = len(np.unique(dataset)) / len(dataset)

        stats = np.hstack((n_char, n_words, p_unique))
        return stats

    @staticmethod
    def apply_on_column_datasets(X_columns, func):
        """
        Method used to apply a function on the fusion of the datasets split in X_columns
        Should return the same structure than X_column with the dataset split again
        """
        merged_dataset = []
        datasets_length = []

        for row in X_columns:
            dataset = row[2]
            merged_dataset += dataset
            datasets_length.append(len(dataset))

        merged_dataset = func(merged_dataset)
        # Convert from sparse matrix to np.array
        merged_dataset = merged_dataset.toarray()

        start = 0
        for i, row in enumerate(X_columns):
            stop = start + datasets_length[i]

            dataset_vectorised = merged_dataset[start:stop]
            ngram_vector = np.sum(dataset_vectorised, axis=0)
            row[2] = ngram_vector

            start = stop

        return X_columns

    @staticmethod
    def transform_to_matrix(X_columns):
        """
        Transform the dataset which is a list of rows, each of which is a tuple.
        Each tuple is changed into a 1-d numpy array, and last the rows are
        concat in a single numpy array
        """
        X = []
        for row in X_columns:
            # TODO: here we drop column name row[0]. Don't.
            col_stat_features = row[1]
            ngram_vector = row[2]

            item = np.hstack((
                col_stat_features.reshape(1, -1),
                ngram_vector.reshape(1, -1)
            ))
            X.append(item)

        return np.concatenate(X)

    def n_gram_fit_transform(self, X_train_col, y_train, ngram_range):
        self.n_gram_vectorizer = TfidfVectorizer(
            analyzer=NGramClassifier.call_find_ngrams(
                ngram_range=ngram_range
            )
        )
        X_train_col = self.apply_on_column_datasets(
            X_train_col,
            self.n_gram_vectorizer.fit_transform
        )

        X_train = self.transform_to_matrix(X_train_col)

        return X_train, np.array(y_train)

    def n_gram_transform(self, X_test_col):
        X_test_col = self.apply_on_column_datasets(
            X_test_col,
            self.n_gram_vectorizer.transform
        )

        X_test = self.transform_to_matrix(X_test_col)

        return X_test

    def extract_n_grams(self, columns):
        n_grams_weighted = {}
        for name, col in columns.items():
            length = len(col)
            for n_gram, weight in self.col_ngrams(col).items():
                weight /= length
                if n_gram not in n_grams_weighted:
                    n_grams_weighted[n_gram] = 0
                n_grams_weighted[n_gram] += weight
        return n_grams_weighted

    @staticmethod
    def call_find_ngrams(ngram_range=(3,)):
        if len(ngram_range) == 2:
            ngram_range = range(ngram_range[0], ngram_range[1] + 1)

        def find_ngrams(cell):
            if isnan(cell):
                return []
            if isinstance(cell, str):
                # Cell global preprocessing:
                # * Rm end point
                if cell[-1] == '.':
                    cell = cell[:-1]

                all_grams = []
                for n in ngram_range:
                    raw_n_grams = zip(*[cell[i:] for i in range(n)])
                    # Remove n_grams with invalid characters like spaces
                    except_chars = [' ', '.', '?', '!']
                    n_grams = [''.join(n_gram) for n_gram in raw_n_grams if notin(n_gram, except_chars)]
                    # Replace some characters like numbers with generic masks
                    n_grams = [re.sub('\d', '\d', n_gram) for n_gram in n_grams]
                    all_grams += n_grams
                return all_grams
            else:
                raise TypeError('Cell should be None, Nan or str but got', type(cell))

        return find_ngrams

    def col_ngrams(self, col, n=3, n_grams=None):
        if n_grams is None:
            n_grams = {}
        for cell in col:
            for n_gram in self.find_ngrams(cell, n):
                if n_gram not in n_grams:
                    n_grams[n_gram] = 1
                else:
                    n_grams[n_gram] += 1
        return n_grams

    def pred2label(self, pred):
        pred = int(pred)
        for icol, label in enumerate(self.labels):
            if icol == pred:
                return label

    def score(self, y_pred, y_test):
        good_pred = 0
        label_acc = {label: {'TP': 0, 'NB': 0, 'FP': 0} for label in list(self.labels)}
        n_pred = len(y_pred)
        for pred, test in zip(y_pred, y_test):
            pred_label = self.pred2label(pred)
            test_label = self.pred2label(test)
            if pred == test:
                good_pred += 1
                label_acc[test_label]['TP'] += 1
            else:
                label_acc[pred_label]['FP'] += 1
            label_acc[test_label]['NB'] += 1
            if pred_label not in label_acc:
                print(pred)

        for label, scores in label_acc.items():
            print('{}\t{}/{}\t   {}% \t(FP:{})'.format(
                (label + ' ' * 20)[:20],
                scores['TP'],
                scores['NB'],
                round(100 * scores['TP'] / scores['NB'], 2) if scores['NB'] != 0 else '-',
                scores['FP']
            ))
        print('SCORE {}/{} :   {}%'.format(good_pred, n_pred, round(100 * good_pred / n_pred, 2)))
