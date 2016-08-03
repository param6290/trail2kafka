__author__ = 'ravi.shekhar'

"""Monitor the app with following purpose.
    1. Track the failed bucket queue
    2. Generate program metrics.
    3. Keep track of the Application Counters.
"""

import skeleton as ds


def worker(parameters):

    # continuously monitor the queue for any new entry and take action appropriately.
    while True:
        if ds.get_failed_bucket_queue_size() > 0:
            # pull out that data for failure scenario send.
            initial_bucket_pointer, final_bucket_pointer = ds.get_failed_bucket_queue().get(True, 1)
            # take action over this fp and lp
            print "Extracted the first Failed Bucket"
            print initial_bucket_pointer
            print final_bucket_pointer

            # open the file pointer here.
            target_file = parameters.configuration_obj.get('FILE_PATH') + parameters.configuration_obj.get('FILE_NAME')
            target_file_handle = target_file.open(target_file, 'r')
            # Extract until the line is reached that has the pointer lesss then the lp.
            target_file_handle.seek(initial_bucket_pointer)
            while True:
                # this line has to be pushed into the master queue.
                # dope the master queue with the current recovering line.
                line = target_file_handle.readline()
                line.strip()
                # lp is the last pointer.
                current_bucket_pointer = target_file_handle.seek()
                if current_bucket_pointer >= final_bucket_pointer:
                    break
                print "Pushing the record into main QUEUE"
                ds.get_master_queue().put(line)

