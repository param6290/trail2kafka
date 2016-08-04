__author__ = 'ravi.shekhar'

"""Consume the master queue and push it to kafka.

"""

from kafka import KafkaProducer
# from kafka.errors import KafkaError
from datetime import datetime
import time

import skeleton as ds

__FIRST_SUCCEEDED_BYTE_MARKER = 0
__FIRST_FAILED_BYTE_MARKER = 0


def fine_callback(*args):
    global __FIRST_SUCCEEDED_BYTE_MARKER
    global __FIRST_FAILED_BYTE_MARKER
    if ds.get_error_indicator():
        ds.set_error_indicator(False)  # reset the error indicator, as evething seems fine now.
        __FIRST_SUCCEEDED_BYTE_MARKER = args[0]
        print "First Succeeded | " + __FIRST_SUCCEEDED_BYTE_MARKER
        fh = open('./meta/FileBucket', 'at')
        failed_bucket = str(__FIRST_FAILED_BYTE_MARKER) + '|' + str(__FIRST_SUCCEEDED_BYTE_MARKER) + "\n"
        fh.write(failed_bucket)
        # Put the failure,success pointer into the failed bucket queue.
        ds.get_failed_bucket_queue().put(str(__FIRST_FAILED_BYTE_MARKER), str(__FIRST_SUCCEEDED_BYTE_MARKER))


def err_callback(*args):
    global __FIRST_FAILED_BYTE_MARKER
    # check if the error switch is on, skip it, as the error has already been detected.
    if not ds.get_error_indicator():
        ds.set_error_indicator(True)
        __FIRST_FAILED_BYTE_MARKER = args[0]
        print "First Failed | " + __FIRST_FAILED_BYTE_MARKER


def worker(parameters):
    consumer_counter = 0
    d = parameters.configuration_obj
    string_of_servers = d.get('KP_bootstrap_servers')
    topic = d.get('KP_topic').strip()
    partitioN = d.get('KP_partition')
    producer = KafkaProducer(bootstrap_servers=map(lambda e: e.strip(), string_of_servers.split(',')), client_id=d.get("KP_client_id"), acks=d.get("KP_acks"), batch_size=d.get("KP_batch_size"), buffer_memory=d.get("KP_buffer_memory"))
    while True:
        consumer_counter += 1
        try:
            record_line = ds.get_master_queue().get(True, 1)
            future = producer.send(topic, value=record_line, partition=partitioN)
            # record_metadata = future.get(timeout=10)
            byte_marker = record_line[:record_line.index(',')]
            future.add_callback(fine_callback, byte_marker)
            future.add_errback(err_callback, byte_marker)

        except:
            # Decide what to do if produce request failed...
            # log this exception
            pass

        if consumer_counter % 50000 == 0:
            print str(datetime.today()) + " | " + str(consumer_counter)
