__author__ = 'ravi.shekhar'

"""This module contains all the utility functions.

"""

from datetime import datetime
import time


def attach_timestamp():
    return datetime.now().strftime("%H:%M:%S.%f")


def indexOf_nth_occurrence(record_line, n, substring):
    """ This function will give the nth occurrence of a substring in the record_line. A typical use can be to find out,
    second occurance of comma in a record line.

    :param record_line:
    :param n:
    :param substring:
    :return:
    """

