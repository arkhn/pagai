import re
import pickle

from sklearn.ensemble import RandomForestClassifier

from .utils import *


class NGramClassifier:
    def __init__(self, n_gram_size=3):
        self.n_gram_size = n_gram_size
        self.clf = RandomForestClassifier(n_estimators=300, max_depth=4, random_state=0)

    def preprocess(self, data_file_path=None):
        if data_file_path is None:
            raise FileExistsError('Please make sure to provide the path of a '
                                  'pickled file which contains a dict with columns. '
                                  'See tutorial notebooks to create such a file.')
        with open(data_file_path, 'rb') as f:
            columns, column_labels = pickle.load(f)

        self.labels = list(set(column_labels.values()))
        self.n_labels = len(self.labels)

        self.label_columns = {label: [] for label in self.labels}
        for column, label in column_labels.items():
            self.label_columns[label].append(column)

        n_grams_weighted = self.extract_n_grams(columns)

        limit = 0.01  # this is arbitrary -> tfidf and n_feat limit instead
        n_grams_feat = [k for k, v in n_grams_weighted.items() if v > limit]
        print('ngrams kept:', len(n_grams_feat), '/', len(n_grams_weighted))

        X_train, y_train, X_test, y_test = self.build_datasets(columns, column_labels, n_grams_feat, 1000)

        return X_train, y_train, X_test, y_test

    def fit(self, X_train, y_train):
        self.clf.fit(X_train, y_train)

    def predict(self, X_test):
        y_pred = self.clf.predict(X_test)
        return y_pred

    def build_datasets(self, columns, column_labels, n_grams_feat, n_train=1000):
        test_frac = 0.2
        n_items = int(n_train * (1 + test_frac))
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

        n_feats = len(n_grams_feat)

        X_train, y_train = [], []
        X_test, y_test = [], []
        for i, item in enumerate(items):
            cell, icol = item
            n_grams = self.find_ngrams(cell)
            feats = [j for j, n_gram in enumerate(n_grams_feat) if n_gram in n_grams]
            if i < n_train:
                X_train.append(one_hot_vector(feats, n_feats))
                y_train.append(icol)
            else:
                X_test.append(one_hot_vector(feats, n_feats))
                y_test.append(icol)

        return np.array(X_train), np.array(y_train), np.array(X_test), np.array(y_test)

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

    def find_ngrams(self, cell, n=3):
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
