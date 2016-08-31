__author__ = 'ravi.shekhar'

"""Host's Features for reading the data from file and pushing it to master queue.

"""

import time
from datetime import datetime

import skeleton as ds


def _attach_timestamp():
    return datetime.now().strftime("%H:%M:%S.%f")


def follow_from(source_file_handle, fp_position):
    source_file_handle.seek(fp_position)
    while True:
        curr_position = source_file_handle.tell()
        line = source_file_handle.readline()
        if not line:
            time.sleep(0.5)
            continue
        yield (str(curr_position), line)


def __normal_extraction_logic(parameters):
    # Initialize Counter for serial number of the records.
    record_serial_number = parameters.last_successful_serial_number
    generator_handle = follow_from(parameters.source_file_handle, parameters.initial_pointer)
    for curr_position, line in generator_handle:
        record_serial_number.increment()
        my_line = curr_position + ',' + str(record_serial_number.get_counter()) + ',' + _attach_timestamp() + ',' + line
        my_line = my_line.strip()
        try:
            ds.get_master_queue().put(my_line, True)
        except:
            # This has to be logged. what exception could occur here.
            pass


def __recovery_extraction_logic(parameters):
    # initializing the counter for serial number of the records.
    record_serial_number = parameters.last_successful_serial_number
    generator_handle = follow_from(parameters.source_file_handle, parameters.initial_pointer)
    for curr_position, line in generator_handle:
        record_serial_number.increment()
        my_line = curr_position + ',' + str(record_serial_number.get_counter()) + ',' + _attach_timestamp() + ',' + line
        my_line = my_line.strip()
        try:
            ds.get_master_queue().put(my_line, True)
        except:
            # This has to be logged. what exception could occur here.
            print "Exception Occurred | While Pushing Into The Queue"
            pass


#  The logic in here can also send half the line. coz you are checking the final_pointer with <= condition.
#  If you make it strictly ==, the condition should match exactly, if the input goes wrong. it will be a disaster.
def __bucket_extraction_logic(parameters):
    generator_handle = follow_from(parameters.source_file_handle, parameters.initial_pointer)
    for curr_position, line in generator_handle:
        #  If we have reached the end of the bucket we need to quit.
        if parameters.final_pointer <= int(curr_position):
            # Signal other threads that the bucket has completed.
            print "Bucket Fetched"
            break
        my_line = curr_position + ',' + line
        my_line = my_line.strip()
        try:
            print my_line
            ds.get_master_queue().put(my_line, True)
        except:
            print "Exception Occurred"


def worker(parameters):
    # log information here.

    #  Determine Mode and take appropriate action.

    if parameters.execution_mode == "normal":
        __normal_extraction_logic(parameters)
    if parameters.execution_mode == "recovery":
        __recovery_extraction_logic(parameters)
    if parameters.execution_mode == "bucket":
        __bucket_extraction_logic(parameters)
