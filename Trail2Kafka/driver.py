__author__ = 'ravi.shekhar'

"""Drive the Application.

   The purpose of this module is to drive the application components through a single doorway.
"""

import sys
import threading

import confighelper as ch
import skeleton as ds
import ntaproducer
import ntaconsumer
import monitor

from globalvar import PROJECT_ROOT
from recovery import recoverytool as rt


# Internal Function
def __initiate_threads(parameters):
    producer_thread = threading.Thread(name='PRODUCER', target=ntaproducer.worker, args=(parameters,))
    consumer_thread = threading.Thread(name='CONSUMER', target=ntaconsumer.worker, args=(parameters,))
    #app_monitor_thread = threading.Thread(name='App MONITOR THREAD', target=monitor.worker, args=(parameters,))
    # clock_thread = threading.Thread(name='NTA-CLOCK',target=ntaclock.clock_func,args=(TERMINATE_SIGNAL,))

    # Demonize the threads.
    producer_thread.daemon = True
    consumer_thread.daemon = True
    #app_monitor_thread.daemon = True
    # ntaclock_thread.daemon = True

    # Start the threads
    producer_thread.start()
    consumer_thread.start()
    #app_monitor_thread.start()

    # Perform the Join.
    consumer_thread.join()


# Internal Function : Get source file handle.
# What should be the correct way of writing this function. can you see the scope confusion. If I have to declare
# _source_file_handle before the try statement. What am i going to do if an exception occurs here. will this function
# return the empty handler ?
def __get_file_handle(config_dict):
    _source_file_handle = ''
    try:
        file_pointer = config_dict.get("FILE_PATH") + config_dict.get("FILE_NAME")
        _source_file_handle = open(file_pointer, mode='rt')
    except IOError as e:
        # log the error here.
        print "Error Occurred While opening the file : {}".format(str(e))

    # what should be the correct way of writing this function ??
    return _source_file_handle


# Internal Function
def run_normal_mode(parameters):
    """Execute the application in normal mode.

    :param parameters:
    :return:
    """
    # Do something that is required for normal mode and then call the function
    __initiate_threads(parameters)


def run_recovery_mode(parameters):
    """Execute the application in Recovery mode.

    :param parameters:
    :return:
    """
    # Do something here that is required for the recovery mode.
    __initiate_threads(parameters)


def run_bucket_mode(parameters):
    """Execute the Applicaiton in Bucket Mode.

    :param parameters:
    :return:
    """
    # Do something here that is required for the bucket mode.
    __initiate_threads(parameters)


def _cleanup():
    """This function will clean up the resources for Application Initialization.
    :return:
    """
    #1. clean FileBucket
    #2. other settings will follow.

    # clean up the FileBucket File for the normal run.
    try:
        fh_filebucket = open(PROJECT_ROOT + "meta/FileBucket", "w")
        fh_filebucket.close()
    except IOError:
        print "Exception Occurred while bucket file cleanup"
        pass
    # clean up the recovery_point file.
    try:
        fh_recoverypoint = open(PROJECT_ROOT + "meta/recovery_point", "w")
        fh_recoverypoint.close()
    except IOError:
        print "Exception Occurred while recovery point cleanup"
        pass


def app_driver(argv):
    """
    Main Function to drive the application.
    :return:
    """

    # This block of code should go in initialization function.
    non_pollable_configuration = ch.NonPollableConfiguration()
    non_pollable_configuration.parse_config()
    config_dict = non_pollable_configuration.get_conf_dict()

    source_file_handle = __get_file_handle(config_dict)
    ds.initialize_queue(config_dict.get('MASTER_QUEUE_SIZE'))
    ds.initialize_failed_bucket_queue(config_dict.get('FAILED_BUCKET_QUEUE_SIZE'))
    ds.initialize_terminate_signal()

    # This block of code will create a parameter object for the application.
    # This step is very crucial.

    # Identifying Mode
    if len(sys.argv) == 1:
        # log this.
        print "Error!! Please supply the mode <normal/recovery/bucket>"
    else:
        execution_mode = sys.argv[1]
        if execution_mode == "normal":
            print "Executing Application in NORMAL Mode"
            parameters = ds.Parameters('normal', source_file_handle, 0, None, ds.get_terminate_signal, config_dict)
            parameters.last_successful_serial_number = ds.Counter(0)
            _cleanup()
            run_normal_mode(parameters)
        elif execution_mode == "recovery":
            # This is the place to handle the cmd line argument. Take the initial pointer from the system.
            print "Running program in recovery mode"
            # extract recovery params.
            recovery_topic = config_dict.get('KP_topic')
            recovery_partition = config_dict.get('KP_partition')
            last_commit_offset = rt.get_last_offset(recovery_topic, recovery_partition)
            print recovery_topic
            print recovery_partition
            print last_commit_offset
            recovery_param = rt.RecoveryParams(recovery_topic, recovery_partition, last_commit_offset)
            last_successful_byte_marker, last_successful_serial_number = tuple([ int(item) for item in rt.recover(recovery_param).split(',')])
            print "Last successful Byte Marker : " + str(last_successful_byte_marker)
            print "Last successful Serial Number : " + str(last_successful_serial_number)

            parameters = ds.Parameters('recovery', source_file_handle, last_successful_byte_marker, None, ds.get_terminate_signal, config_dict)
            parameters.last_successful_serial_number = ds.Counter(int(last_successful_serial_number))
            run_recovery_mode(parameters)
        elif "bucket" == execution_mode:
            parameters = ds.Parameters('bucket', source_file_handle, 0, None, ds.get_terminate_signal, config_dict)
            # binding new attributes.
            parameters.initial_pointer = int(sys.argv[2])
            parameters.final_pointer = int(sys.argv[3])
            run_bucket_mode(parameters)


if __name__ == "__main__":
    app_driver(sys.argv)
