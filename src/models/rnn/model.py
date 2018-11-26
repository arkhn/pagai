import torch
import torch.nn as nn
import matplotlib.pyplot as plt
from .utils import *


class RNN(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super().__init__()

        self.hidden_size = hidden_size

        self.i2h = nn.Linear(input_size + hidden_size, hidden_size)
        self.i2n = nn.Linear(input_size + hidden_size, hidden_size)
        self.n2n = nn.Linear(hidden_size, hidden_size)
        self.n2o = nn.Linear(hidden_size, output_size)
        self.activation = nn.ReLU()
        self.softmax = nn.LogSoftmax(dim=1)

    def forward(self, input, hidden):
        combined = torch.cat((input, hidden), 1)
        hidden = self.i2h(combined)
        output = self.i2n(combined)
        output = self.n2n(output)
        output = self.n2o(output)
        output = self.activation(output)
        output = self.softmax(output)
        return output, hidden

    def init_hidden(self):
        return torch.zeros(1, self.hidden_size)


class RNNClassifier(nn.Module):
    def __init__(self, model, n_hidden, criterion=nn.NLLLoss(), learning_rate=0.002):
        super().__init__()
        self.labels = []
        self.category_lines = {}
        self.model = model
        self.n_hidden = n_hidden
        self.criterion = criterion
        self.learning_rate = learning_rate

    def preprocess(self, columns, labels, test_only=False):
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

    def fit(self, X_train, y_train, n_iter=100000, verbose=True, print_every=5000, plot_every=500):

        for i, label in enumerate(y_train):
            self.category_lines[label] = []

        for i, label in enumerate(y_train):
            self.category_lines[label] += list(
                filter(None, [unicodeToAscii(x.lower()) for x in X_train[i][1]]))

        max_seq_lengths = []
        for i, label in enumerate(y_train):
            max_seq_lengths.append(max([len(x) for x in self.category_lines[label]]))

        self.all_categories = list(self.category_lines.keys())
        self.n_categories = len(self.all_categories)
        self.max_length = max(max_seq_lengths)

        current_loss = 0
        self.all_losses = []

        self.rnn = self.model(n_letters, self.n_hidden, self.n_categories)
        start = time.time()

        for iter in range(1, n_iter + 1):
            category, line, category_tensor, line_tensor = self.random_training_example()
            output, loss = self.train(category_tensor, line_tensor)
            current_loss += loss

            # Print iter number, loss, name and guess
            if (verbose == True) and (iter % print_every == 0):
                guess, guess_i = self.category_from_output(output)
                correct = '✓' if guess == category else '✗ (%s)' % category
                print('%d %d%% (%s) %.4f %s / %s %s' % (
                iter, iter / n_iter * 100, timeSince(start), loss, line, guess, correct))

            # Add current loss avg to list of losses
            if iter % plot_every == 0:
                self.all_losses.append(current_loss / plot_every)
                current_loss = 0

    def train(self, category_tensor, line_tensor):
        hidden = self.rnn.init_hidden()

        self.rnn.zero_grad()

        for i in range(line_tensor.size()[0]):
            output, hidden = self.rnn(line_tensor[i], hidden)

        loss = self.criterion(output, category_tensor)
        loss.backward()

        # Add parameters' gradients to their values, multiplied by learning rate
        for p in self.rnn.parameters():
            p.data.add_(-self.learning_rate, p.grad.data)

        return output, loss.item()

    def predict(self, X_test, sampling = 0.2):
        y_pred = []
        for i in range(len(X_test)):
            y_pred.append(self.predict_column(X_test[i], sampling))
        return y_pred

    def predict_column(self, column, sampling=0.2):
        sample_length = int(sampling * len(column[1]))
        sample_prediction = []
        for i in range(sample_length):
            try:
                sample_prediction.append(
                    self.predict_sample(randomChoice(column[1]), n_predictions=1, verbose=False)[0][1])
            except UnboundLocalError:
                sample_prediction.append('UNKNOWN')
        return max(sample_prediction, key=sample_prediction.count)

    def predict_sample(self, input_line, n_predictions=1, verbose=True):
        with torch.no_grad():
            output = self.evaluate(lineToTensor(input_line))

            # Get top N categories
            topv, topi = output.topk(n_predictions, 1, True)
            predictions = []

            if verbose:
                print('\n> %s' % input_line)
            for i in range(n_predictions):
                value = topv[0][i].item()
                category_index = topi[0][i].item()
                if verbose:
                    print('(%.2f) %s' % (value, self.all_categories[category_index]))
                predictions.append([value, self.all_categories[category_index]])
        return predictions

    def plot_losses(self):
        plt.figure()
        plt.plot(self.all_losses)

    def category_from_output(self, output):
        top_n, top_i = output.topk(1)
        category_i = top_i[0].item()
        return self.all_categories[category_i], category_i

    def random_training_example(self):
        category = randomChoice(self.all_categories)
        line = randomChoice(self.category_lines[category])
        category_tensor = torch.tensor([self.all_categories.index(category)], dtype=torch.long)
        line_tensor = lineToTensor(line)
        return category, line, category_tensor, line_tensor

    def evaluate(self, line_tensor):
        hidden = self.rnn.init_hidden()

        for i in range(line_tensor.size()[0]):
            output, hidden = self.rnn(line_tensor[i], hidden)
        return output

    def score(self, y_pred, y_test):
        """
        Utility function to build accuracy and false positive (FP) scores per class
        """
        good_pred = 0
        label_acc = {label: {'TP': 0, 'NB': 0, 'FP': 0} for label in list(self.labels)}
        n_pred = len(y_pred)
        for pred, test in zip(y_pred, y_test):
            if pred == test:
                good_pred += 1
                label_acc[test]['TP'] += 1
            else:
                label_acc[pred]['FP'] += 1
            label_acc[test]['NB'] += 1
            if pred not in label_acc:
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
            X.append([column_name, dataset])
            y.append(label)

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
