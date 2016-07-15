__author__ = 'ravi.shekhar'

"""This module contain the necessary data structures used in the project.

Attributes:

"""

import Queue
import threading

from kafka.common import TopicPartition

__all__ = ['initialize_queue']

# This Queue is responsible to tie up the producer/consumer model.
# The size of this Queue is fixed at startup of the application.
__MASTER_QUEUE = ''
__TERMINATE_SIGNAL = ''

__ERROR_INDICATOR = False


def get_error_indicator():
    """retrieve the value of ERROR_INDICATOR.

    :return:
    """
    return __ERROR_INDICATOR


def set_error_indicator(boolvalue):
    """toggle the value of ERROR_INDICATOR.

    :param boolvalue:
    :return:
    """
    global __ERROR_INDICATOR
    __ERROR_INDICATOR = boolvalue


def initialize_queue(master_queue_size):
    """Initialize master queue.
    :param master_queue_size:
    :return:
    """
    global __MASTER_QUEUE
    __MASTER_QUEUE = Queue.Queue(master_queue_size)


def initialize_terminate_signal():
    """Initialize the Terminate Signal, which will stop the threads when triggered.

    :return:
    """
    global __TERMINATE_SIGNAL
    __TERMINATE_SIGNAL = threading.Event()


def get_terminate_signal():
    """

    :return:
    """
    return __TERMINATE_SIGNAL


def get_master_queue_size():
    """Return the current size of the queue.

    Depicts the stress level.

    :return:
    """
    return __MASTER_QUEUE.qsize()


def get_master_queue():
    """Get the pointer to the master queue.
    :return: __MASTER_QUEUE
    """
    return __MASTER_QUEUE


class Parameters(object):
    """Class to hold all the parameters in the application.

    """

    _DEFAULT_CONFIG = {
        'execution_mode': 'normal',
        'initial_pointer': 0,
        'final_pointer': None,
        'configuration_obj': None
    }

    # do you want to do it like in kafka api. using the **configs dictionary.
    def __init__(self, execution_mode, source_file_handle, initial_pointer, final_pointer, terminate_signal,
                 configuration_obj):
        self.execution_mode = execution_mode
        self.source_file_handle = source_file_handle
        self.initial_pointer = initial_pointer
        self.final_pointer = final_pointer
        self.terminate_signal = terminate_signal
        self.configuration_obj = configuration_obj
