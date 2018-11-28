import unicodedata
import string
import random
import time
import math
import torch


all_letters = (
    string.ascii_lowercase + string.punctuation + string.digits + string.whitespace
)
n_letters = len(all_letters)


def unicodeToAscii(s):
    return "".join(
        c
        for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn" and c in all_letters
    )


# Find letter index from all_letters, e.g. "a" = 0
def letterToIndex(letter):
    return all_letters.find(letter)


# Turn a line into a <line_length x 1 x n_letters>,
# or an array of one-hot letter vectors
def lineToTensor(line):
    conv = unicodeToAscii(line.lower())
    tensor = torch.zeros(len(conv), 1, n_letters)
    for li, letter in enumerate(conv):
        tensor[li][0][letterToIndex(letter)] = 1
    return tensor


def randomChoice(l):
    return l[random.randint(0, len(l) - 1)]


def timeSince(since):
    now = time.time()
    s = now - since
    m = math.floor(s / 60)
    s -= m * 60
    return "%dm %ds" % (m, s)
