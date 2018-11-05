import re
import pickle

from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer

from .utils import *


class NGramClassifier:
    def __init__(self, n_gram_size=3):
        self.n_gram_size = n_gram_size
        self.clf = RandomForestClassifier(n_estimators=300, max_depth=4, random_state=0)

    def preprocess(self, columns, column_labels):
        # Dict columns can have nested lists, we create one key for each of them
        extended_columns = {}
        extended_column_labels = {}
        for name, column in columns.items():
            for i, dataset in enumerate(column):
                col_name = name
                if len(column) > 1:
                    col_name = '{}#{}'.format(name, i+1)
                extended_columns[col_name] = dataset
                extended_column_labels[col_name] = column_labels[name]
        columns = extended_columns
        column_labels = extended_column_labels

        self.labels = list(set(column_labels.values()))
        self.n_labels = len(self.labels)

        self.label_columns = {label: [] for label in self.labels}
        for column, label in column_labels.items():
            self.label_columns[label].append(column)

        # n_grams_weighted = self.extract_n_grams(columns)
        #
        # limit = 0.01  # this is arbitrary -> tfidf and n_feat limit instead
        # n_grams_feat = [k for k, v in n_grams_weighted.items() if v > limit]
        # print('ngrams kept:', len(n_grams_feat), '/', len(n_grams_weighted))

        X_train, y_train, X_test, y_test = self.build_datasets(columns, column_labels)

        return X_train, y_train, X_test, y_test

    def fit(self, X_train, y_train, ngram_range=(2, 4)):
        X_train, y_train = self.n_gram_fit_transform(X_train, y_train, ngram_range)
        self.clf.fit(X_train, y_train)

    def predict(self, X_test):
        X_test = self.n_gram_transform(X_test)
        y_pred = self.clf.predict(X_test)
        return y_pred

    def build_datasets(self, columns, column_labels, n_items=1000):
        items = []

        column_lengths = {column_name: len(col) for column_name, col in columns.items()}

        while len(items) < n_items:
            ilabel = np.random.randint(self.n_labels)
            label = self.labels[ilabel]
            n_label_cols = len(self.label_columns[label])

            ilabelcol = np.random.randint(n_label_cols)
            column_name = self.label_columns[label][ilabelcol]
            column_length = column_lengths[column_name]

            icell = np.random.randint(column_length)
            cell = columns[column_name][icell]

            label = column_labels[column_name]
            ilabel = [i for i, l in enumerate(self.labels) if l == label][0]
            if not isnan(cell):
                items.append((cell, ilabel))

        X_train, y_train = [], []
        X_test, y_test = [], []
        for i, item in enumerate(items):
            cell, icol = item
            if i < 0.8 * n_items:
                X_train.append(cell)
                y_train.append(icol)
            else:
                X_test.append(cell)
                y_test.append(icol)

        return X_train, y_train, X_test, y_test

    def n_gram_fit_transform(self, X_train, y_train, ngram_range):
        self.vectorizer = TfidfVectorizer(
            analyzer=NGramClassifier.call_find_ngrams(
                ngram_range=ngram_range
            )
        )
        X = self.vectorizer.fit_transform(X_train)
        print(X.shape)
        return X.toarray(), np.array(y_train)

    def n_gram_transform(self, X_test):
        X = self.vectorizer.transform(X_test)
        print(X.shape)
        return X.toarray()

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
            if isinstance(cell, float):
                cell = int(cell)
            if isinstance(cell, int):
                cell = str(cell)
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
                raise TypeError('TYPE', type(cell))

        return find_ngrams

    def find_ngrams(self, cell, n=3):
        """
        OBSOLETE
        """
        if isnan(cell):
            return []
        if isinstance(cell, float):
            cell = int(cell)
        if isinstance(cell, int):
            cell = str(cell)
        if isinstance(cell, str):
            all_grams = zip(*[cell[i:] for i in range(n)])
            # Remove n_grams avec invalid characters like spaces
            except_chars = [' ']
            n_grams = [''.join(n_gram) for n_gram in all_grams if notin(n_gram, except_chars)]
            # Replace some characters like numbers with generic masks
            n_grams = [re.sub('\d', '\d', n_gram) for n_gram in n_grams]
            return n_grams
        else:
            raise TypeError('TYPE', type(cell))

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
