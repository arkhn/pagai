import random
import logging
import numpy as np
import os
import torch.nn as nn


class BaseClassifier(nn.Module):
    """
    The BaseClassifier is the Parent Class of all classifiers. It is use
    to factorize methods that we expect all classifiers to have.
    """

    def __init__(self):
        super().__init__()
        self.classification = None

    def find_all(self, resource_type, max_col_len=20):
        if self.classification is None:
            raise TypeError("Model is not ready for live classification.")
        results = []
        for column in self.classification:
            if resource_type in column.proba_classes:
                column.score = column.proba_classes[resource_type]
            else:
                column.score = 1
                logging.warning("No ResourceType was provided")
            if column.score > float(os.getenv("MIN_SCORE")):
                column.data = column.data[:max_col_len]
                results.append(column)

        return results

    def build_datasets(self, columns, labels, test_only):
        """
        Add stat features about the column dataset and split into train/test
        """
        if test_only:
            X = []
            for column in columns:
                column_name, dataset = column
                stat_features = self.add_stat_features(dataset)
                X.append([column_name, stat_features, dataset])
            n_items = len(X)
            ix = list(range(n_items))
            random.shuffle(ix)
            X_test = []
            reordered_columns = []
            for i in range(n_items):
                X_test.append(X[ix[i]])
                reordered_columns.append(columns[ix[i]])
            return X_test, reordered_columns

        X, y = [], []
        for column, label in zip(columns, labels):
            column_name, dataset = column
            # y.append(label)
            ilabel = [i for i, l in enumerate(self.labels) if l == label][0]
            stat_features = self.add_stat_features(dataset)
            X.append([column_name, stat_features, dataset])
            y.append(ilabel)

        X_train, y_train = [], []
        X_test, y_test = [], []
        reordered_columns = []
        n_items = len(X)
        ix = list(range(n_items))
        random.shuffle(ix)

        for i in range(n_items):
            reordered_columns.append(columns[ix[i]])
            if i < 0.8 * n_items:
                X_train.append(X[ix[i]])
                y_train.append(y[ix[i]])
            else:
                X_test.append(X[ix[i]])
                y_test.append(y[ix[i]])

        return (X_train, y_train, X_test, y_test), reordered_columns

    @staticmethod
    def add_stat_features(dataset):
        """
        Add several stats features about the dataset of each column
        """
        # 1. len of the row
        n_char = np.vectorize(len)(dataset).mean()
        # 2. Nb of words
        n_words = np.vectorize(lambda x: len(x.split(" ")))(dataset).mean()
        # 3. Percentage of unique values / all values
        p_unique = len(np.unique(dataset)) / len(dataset)

        stats = np.hstack((n_char, n_words, p_unique))
        return stats

    def pred2label(self, pred):
        """
        Convert label codes used for training to the real string value
        """
        pred = int(pred)
        for icol, label in enumerate(self.labels):
            if icol == pred:
                return label

    def score(self, y_pred, y_test):
        """
        Utility function to build accuracy and false positive (FP) scores per class
        """
        good_pred = 0
        label_acc = {label: {"TP": 0, "NB": 0, "FP": 0} for label in list(self.labels)}
        n_pred = len(y_pred)
        for pred, test in zip(y_pred, y_test):
            pred_label = self.pred2label(pred)
            test_label = self.pred2label(test)
            if pred == test:
                good_pred += 1
                label_acc[test_label]["TP"] += 1
            else:
                label_acc[pred_label]["FP"] += 1
            label_acc[test_label]["NB"] += 1
            if pred_label not in label_acc:
                print(pred)

        for label, scores in label_acc.items():
            print(
                "{}\t{}/{}\t   {}% \t(FP:{})".format(
                    (label + " " * 20)[:20],
                    scores["TP"],
                    scores["NB"],
                    round(100 * scores["TP"] / scores["NB"], 2)
                    if scores["NB"] != 0
                    else "-",
                    scores["FP"],
                )
            )
        print(
            "SCORE {}/{} :   {}%".format(
                good_pred, n_pred, round(100 * good_pred / n_pred, 2)
            )
        )
