__author__ = 'ravi.shekhar'

"""This module contains all the utility functions.

"""

from datetime import datetime
import time


def attach_timestamp():
    """ Returns the current timestamp
    """
    return datetime.now().strftime("%H:%M:%S.%f")


def indexSpecial(record_line, n, s):
    """
    Returns the index of the nth occurrence of the s substring in the string record_line.
    :param record_line:
    :param n:
    :param s:
    :return:
    """
    ind = 0
    ind1 = 0
    for i in range(0, n):
            ind = record_line.index(s)
            ind_plus_1 = ind + 1
            ind1 += ind_plus_1
            record_line = record_line[ind_plus_1:]
    return ind1-1

