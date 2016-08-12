__author__ = 'ravi.shekhar'

"""Consume the master queue and push it to kafka.

"""

from kafka import KafkaProducer
# from kafka.errors import KafkaError
from datetime import datetime
import time

from globalvar import PROJECT_ROOT
import skeleton as ds

_FIRST_SUCCEEDED_TUPLE = ''
_FIRST_FAILED_TUPLE = ''

bucket = ds.Bucket()

_FIRST_FAILED_TUPLE = 0
_FIRST_SUCCEEDED_TUPLE = 0


def fine_callback(*args):
    global _FIRST_FAILED_TUPLE
    global _FIRST_SUCCEEDED_TUPLE
    if ds.get_error_indicator():
        print "Error Indicator Changed."
        _FIRST_SUCCEEDED_TUPLE = args
        ds.set_error_indicator(False)  # reset the error indicator, as everything seems fine now.
        print "First Succeeded | " + _FIRST_SUCCEEDED_TUPLE[0]
        fh = open(PROJECT_ROOT + 'meta/FileBucket', 'at')
        failed_bucket = str(_FIRST_FAILED_TUPLE[0]) + '|' + str(_FIRST_FAILED_TUPLE[1]) \
                        + '|' + str(_FIRST_SUCCEEDED_TUPLE[0]) + '|'  + str(_FIRST_SUCCEEDED_TUPLE[1]) + "\n"
        print "Pushing the below Tuple in the Failure Queue"
        ds.get_failed_bucket_queue().put(failed_bucket)
        fh.write(failed_bucket)


def err_callback(*args):
    global _FIRST_FAILED_TUPLE
    # check if the error switch is on, skip it, as the error has already been detected.
    if not ds.get_error_indicator():
        ds.set_error_indicator(True)
        _FIRST_FAILED_TUPLE = args
        print "First Failed | " + _FIRST_FAILED_TUPLE[0]


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

            # Find the first comma and get the byte pointer. Then find the second comma and get the serial number.
            i1 = record_line.index(',')
            byte_marker = record_line[:i1]
            record_serial_number = record_line[i1+1:][:record_line[i1+1:].index(',')]

            future.add_callback(fine_callback, byte_marker, record_serial_number)
            future.add_errback(err_callback, byte_marker, record_serial_number)

        except:
            # Decide what to do if produce request failed...
            # log this exception
            pass

        if consumer_counter % 50000 == 0:
            print str(datetime.today()) + " | " + str(consumer_counter)
