__author__ = 'ravi.shekhar'

"""Monitor the app with following purpose.
    1. Track the failed bucket queue
    2. Generate program metrics.
    3. Keep track of the Application Counters.
"""

import skeleton as ds
import utils


def worker(parameters):
    # continuously monitor the queue for any new entry and take action appropriately.
    failed_bucket_counter = ds.Counter(0)
    while True:
        if ds.get_failed_bucket_queue_size() > 0:
            # pull out that data for failure scenario send.
            failed_bucket_size_counter = ds.Counter(0)
            # initial_bucket_pointer, final_bucket_pointer = ds.get_failed_bucket_queue().get(True, 1)
            try:
                failed_bucket = ds.get_failed_bucket_queue().get(True)
                failed_bucket_counter.increment()
            except:
                pass
            # take action over this fp and lp
            print "Extracted the first Failed Bucket"
            print failed_bucket

            initial_bucket_pointer = int(failed_bucket.split('|')[0])
            initial_bucket_pointer_serial_number = int(failed_bucket.split('|')[1])
            final_bucket_pointer = int(failed_bucket.split('|')[2])
            final_bucket_pointer_serial_number = int(failed_bucket.split('|')[3])
            # open the file pointer here.
            print "Opening the file to read"
            target_file = parameters.configuration_obj.get('FILE_PATH') + parameters.configuration_obj.get('FILE_NAME')
            print target_file
            target_file_handle = open(target_file, 'r')
            # Extract until the line is reached that has the pointer less then the lp.
            print "Pointing to read the contents"
            target_file_handle.seek(initial_bucket_pointer)
            while True:
                # this line has to be pushed into the master queue.
                # dope the master queue with the current recovering line.
                current_bucket_pointer = target_file_handle.tell()

                line = target_file_handle.readline()
                line.strip()
                line = str(current_bucket_pointer)+ str(initial_bucket_pointer_serial_number) + utils.attach_timestamp()+line
                initial_bucket_pointer_serial_number += 1
                # lp is the last pointer.
                if current_bucket_pointer >= final_bucket_pointer:
                    print "All Records in the Bucket Pushed"
                    print "FailedBucketCount : {0} | FailedBucketSize : {1}"\
                        .format(failed_bucket_counter.get_counter(), failed_bucket_size_counter.get_counter())
                    failed_bucket_size_counter.reset()
                    break
                print "Pushing the record into main QUEUE"
                failed_bucket_size_counter.increment()
                ds.get_master_queue().put(line)
            target_file_handle.close()
