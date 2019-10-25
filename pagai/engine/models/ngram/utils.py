import math

import numpy as np


def isnan(el):
    return el is None or el == "" or isinstance(el, float) and math.isnan(el)


def notin(list_chars, except_chars):
    return not any([except_char in list_chars for except_char in except_chars])


def one_hot_vector(one_indices, n):
    """
    Convert a list of indices in a one hot vector of length n
    Example:

        [0, 2], 4 -> [1, 0, 1, 0]
    """
    if not isinstance(one_indices, list):
        one_indices = [one_indices]
    vector = np.zeros(n)
    for idx in one_indices:
        vector[idx] = 1
    return vector
