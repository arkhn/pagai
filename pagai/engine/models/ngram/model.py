import re

from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer

from pagai.engine.models.base import BaseClassifier
from .utils import *


class NGramClassifier(BaseClassifier):
    def __init__(self):
        super().__init__()
        self.ngram_range = ()
        self.labels = []
        self.classification = None
        self.n_gram_vectorizer = None
        self.clf = RandomForestClassifier(
            n_estimators=300, max_depth=50, max_features="sqrt", random_state=0
        )

    def __getstate__(self):
        """
        Utility method for pickle.dump
        We need to remove the analyzer because of some error.
        """
        self.n_gram_vectorizer.analyzer = None
        state = {
            "labels": self.labels,
            "ngram_range": self.ngram_range,
            "n_gram_vectorizer": self.n_gram_vectorizer,
            "clf": self.clf,
            "classification": self.classification,
        }
        return state

    def __setstate__(self, state):
        """
        Utility method for pickle.load
        We need to rebuild the analyzer
        """
        self.labels = state["labels"]
        self.n_gram_vectorizer = state["n_gram_vectorizer"]
        self.clf = state["clf"]
        self.ngram_range = state["ngram_range"]
        self.n_gram_vectorizer.analyzer = NGramClassifier.call_find_ngrams(
            ngram_range=self.ngram_range
        )
        self.classification = state["classification"]

    def preprocess(self, columns, labels=None, test_only=False):
        """
        Reorganise data from the SQL loader, add stats features and split in train/test
        """
        if not test_only:
            self.labels = labels
        X_sets, reordered_columns = self.build_datasets(columns, labels, test_only)
        if test_only:
            return X_sets, reordered_columns
        else:
            return X_sets

    def fit(self, X_train, y_train, ngram_range=(2, 3)):
        self.ngram_range = ngram_range
        X_train, y_train = self.n_gram_fit_transform(X_train, y_train)
        # print(X_train.shape)
        self.clf.fit(X_train, y_train)

    def predict(self, X_test):
        """
        Return for each column the most probable resource_type class code.
        Use pred2label to get the string label.
        """
        X_test = self.n_gram_transform(X_test)
        y_pred = self.clf.predict(X_test)
        return y_pred

    def predict_proba(self, X_test):
        """
        Return for each column a tuple of probabilities, one per class
        """
        X_test = self.n_gram_transform(X_test)
        y_pred_proba = self.clf.predict_proba(X_test)

        self.classes = self.clf.classes_
        return y_pred_proba

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

            item = np.hstack(
                (col_stat_features.reshape(1, -1), ngram_vector.reshape(1, -1))
            )
            X.append(item)

        return np.concatenate(X)

    def n_gram_fit_transform(self, X_train_col, y_train):
        self.n_gram_vectorizer = TfidfVectorizer(
            analyzer=NGramClassifier.call_find_ngrams(ngram_range=self.ngram_range)
        )
        X_train_col = self.apply_on_column_datasets(
            X_train_col, self.n_gram_vectorizer.fit_transform
        )

        X_train = self.transform_to_matrix(X_train_col)

        return X_train, np.array(y_train)

    def n_gram_transform(self, X_test_col):
        X_test_col = self.apply_on_column_datasets(
            X_test_col, self.n_gram_vectorizer.transform
        )

        X_test = self.transform_to_matrix(X_test_col)

        return X_test

    def extract_n_grams(self, columns):
        """
        NOT USED AT THE MOMENT
        Given columns, for each column, get the ngrams found, add it
        to a global counter (n_grams_weighted) with a normalising
        weighting function to give every column the same importance.
        """
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
        """
        Function wrapping the find_ngram function to provide an extra
        argument (ngram_range), which would not be possible otherwise as
        it is called by TfidfVectorizer to be its analyzer.
        :param ngram_range: the desired n_gram settings
        :return: the function that find n_grams with the settings provided
        """
        if len(ngram_range) == 2:
            ngram_range = range(ngram_range[0], ngram_range[1] + 1)

        def find_ngrams(cell):
            """
            Find n_grams in the row of a column (called a cell)
            """
            if isnan(cell):
                return []
            if isinstance(cell, str):
                # Cell global preprocessing:
                # * Rm end point
                if cell[-1] == ".":
                    cell = cell[:-1]

                all_grams = []

                # If cell is a single character
                if len(cell) == 1 and 1 not in ngram_range:
                    unigram = "^{}$".format(cell)
                    all_grams.append(unigram)

                # Then, loop on all kinds of n-gram
                for n in ngram_range:
                    raw_n_grams = zip(*[cell[i:] for i in range(n)])
                    # Remove n_grams with invalid characters like spaces
                    except_chars = [" ", ".", "?", "!"]
                    n_grams = [
                        "".join(n_gram)
                        for n_gram in raw_n_grams
                        if notin(n_gram, except_chars)
                    ]
                    # Replace some characters like numbers with generic masks
                    n_grams = [re.sub("\d", "\\\d", n_gram) for n_gram in n_grams]
                    all_grams += n_grams
                return all_grams
            else:
                raise TypeError("Cell should be None, Nan or str but got", type(cell))

        return find_ngrams

    def col_ngrams(self, col, ngram_range=(3,), n_grams=None):
        """
        NOT USED AT THE MOMENT
        For a given column, for each element (called cell), find all ngrams and
        increment a counter dictionary, where all keys are ngrams found so far
        and values are the number of occurrence
        :param col: the given column
        :param ngram_range: parameters for the ngrams we use
        :param n_grams: the counter dictionary given as argument to have incremental behaviour within cols
        :return: the counter dictionary
        """
        find_ngrams = self.call_find_ngrams(ngram_range)

        if n_grams is None:
            n_grams = {}
        for cell in col:
            for n_gram in find_ngrams(cell):
                if n_gram not in n_grams:
                    n_grams[n_gram] = 1
                else:
                    n_grams[n_gram] += 1
        return n_grams
