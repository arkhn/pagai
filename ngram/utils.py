import math

import numpy as np


def isnan(el):
    return isinstance(el, float) and math.isnan(el)


def notin(list_chars, except_chars):
    return not any([except_char in list_chars for except_char in except_chars])

def one_hot_vector(one_indices, n):
    if not isinstance(one_indices, list):
        one_indices = [one_indices]
    vector = np.zeros(n)
    for idx in one_indices:
        vector[idx] = 1
    return vector