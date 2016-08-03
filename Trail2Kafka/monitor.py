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
            fp, lp = ds.get_failed_bucket_queue().get(True, 1)
            # take action over this fp and lp
            print "Extracted the first Failed Bucket"
            print fp
            print lp
