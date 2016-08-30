__author__ = 'ravi.shekhar'

"""This module contain the necessary data structures used in the project.

Attributes:

"""

import Queue
import threading
import logging

import utils

from kafka.common import TopicPartition

import confighelper as ch


"""Dictionary calling
"""
non_pollable_configuration = ch.NonPollableConfiguration()
non_pollable_configuration.parse_config()
config_dict = non_pollable_configuration.get_conf_dict()


"""Initializing log values for logging
"""
LOG_DIR = config_dict.get('LOG_DIRNAME')
LOG_FILE = config_dict.get('LOG_FILENAME')
LOG_LEVEL = config_dict.get('LOG_LEVEL')


"""Setting up the log handler
"""
LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL, }


logging.basicConfig(
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=LOG_DIR + LOG_FILE,
    filemode='w',
    level=LEVELS.get(LOG_LEVEL, logging.NOTSET))


# Driver logger
driverlogger = logging.getLogger("driver")

# Producer logger
plogger = logging.getLogger("ntaproducer")

# Consumer logger
clogger = logging.getLogger("ntaconsumer")

# Recovery logger
rlogger = logging.getLogger("recovery")


__all__ = ['initialize_queue']

# This Queue is responsible to tie up the producer/consumer model.
# The size of this Queue is fixed at startup of the application.
__MASTER_QUEUE = ''
__FAILED_BUCKET_QUEUE = ''
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


def initialize_failed_bucket_queue(failed_bucket_queue_size):
    """Initialize failed bucket queue. This queue will contain the buckets of failed records.
    :param failed_bucket_queue_size:
    :return:
    """
    global __FAILED_BUCKET_QUEUE
    __FAILED_BUCKET_QUEUE = Queue.Queue(failed_bucket_queue_size)


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


def get_failed_bucket_queue_size():
    """

    :return:
    """
    return __FAILED_BUCKET_QUEUE.qsize()


def get_master_queue():
    """Get the pointer to the master queue.
    :return: __MASTER_QUEUE
    """
    return __MASTER_QUEUE


def get_failed_bucket_queue():
    """Get the pointer to the failed bucket queue.

    :return:
    """
    return __FAILED_BUCKET_QUEUE


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


class Counter(object):
    """This class is the representation of the counter.

    """

    def __init__(self, initial_value):
        self.counter = initial_value

    # Do I need to make this operation thead safe ?? Yes I guess, its necessary.
    def increment(self):
        self.counter += 1

    # Do I need to make this operation thead safe ?? Yes I guess, its necessary.
    def decrement(self):
        self.counter -= 1

    def get_counter(self):
        return self.counter

    def set_counter(self, value):
        self.counter = value

    def reset(self):
        self.counter = 0


class Bucket(object):
    """This class represents any two pointer in the source file. The instance of this class can be used as failed bucket
    For the failed bucket case, we need to follow the rule [first_failed, first_succeeded)

    """

    def __init__(self):
        self.dummy = 0

    def set_first_failed_pointer(self, first_failed_pointer, first_failed_record_serial_number):
        self.first_failed = first_failed_pointer
        self.first_failed_serial_number = first_failed_record_serial_number
        self.first_failed_timestamp = utils.attach_timestamp()

    def set_first_succeeded_pointer(self, first_succeeded_pointer, first_succeeded_record_serial_number):
        self.first_succeeded = first_succeeded_pointer
        self.first_succeeded_serial_number = first_succeeded_record_serial_number
        self.first_succeeded_timestamp = utils.attach_timestamp()

    def get_first_failed_pointer(self):
        """
        Get the first failed record of the bucket.
        :return:
        """
        return self.first_failed

    def get_first_succeeded_pointer(self):
        """
        Get the first successfull record off the bucket.
        :return:
        """
        return self.first_succeeded

    def get_bucket_size(self):
        """
        Get the size of the bucket, as in the number of records in this bucket.
        :return:
        """
        return self.bucket_size

    def get_bucket_created_time(self):
        """
        Get the time when this bucket was initialized.
        :return:
        """
        return self.bucket_created_time