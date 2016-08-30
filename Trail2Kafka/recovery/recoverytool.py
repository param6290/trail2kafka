__author__ = 'ravi.shekhar'

import time

from kafka.consumer import KafkaConsumer
from kafka.common import TopicPartition
from kafka.consumer.subscription_state import ConsumerRebalanceListener

from ..globalvar import PROJECT_ROOT

consumer = KafkaConsumer(group_id='nta-group',bootstrap_servers=['192.168.156.55:9092','192.168.156.56:9092',
                                                      '192.168.156.57:9092','192.168.156.58:9092',
                                                      '192.168.156.59:9092','192.168.156.59:9092',
                                                      '192.168.156.60:9092'])


class RecoveryParams(object):
    """ Parameters required for recovery tool.

    """

    def __init__(self, topic, partition, offset):
        self.topic = topic
        self.partition = partition
        self.offset = offset

    # This guy wanted to be @property, as per PEP.
    def get_topic_partition(self):
        return TopicPartition([self.topic], self.partition)


class _CBL(ConsumerRebalanceListener):

    def __init__(self, topic, partition, offset):
        super(_CBL, self).__init__()
        self.topic = topic
        self.partition = partition
        self.offset = offset

    def on_partitions_assigned(self, assigned):
        for topic_partition in assigned:
            if topic_partition[1] == 0:
                print "Topic Partition : " + str(topic_partition[0])
                print "Topic : " + str(topic_partition[1])
                print "Adjusting the seek pointer for retrieval"
                try:
                    tp = TopicPartition(self.topic, self.partition)
                    #consumer.seek(TopicPartition('ravitest', 0), 1585247)
                    consumer.seek(tp, self.offset)
                except TypeError as e:
                    print str(e)
                except:
                    pass

    def on_partitions_revoked(self, revoked):
        pass


def get_last_offset(recovery_topic, recovery_partition):
    """
    :return: Last successfull offset.
    """
    print "Retrieving the last offset"
    print PROJECT_ROOT
    META_LOCATION_rp = PROJECT_ROOT + "/meta/recovery_point"
    import os
    CMD = r'ssh ntart@192.168.156.56 /usr/bin/kafka-run-class kafka.tools.GetOffsetShell --broker-list 192.168.156.55:9092 --topic '+recovery_topic+ ' --partition ' + str(recovery_partition) + " --time -1 | awk -F : '{print $3}' > " + META_LOCATION_rp
    os.system(CMD)
    fh = open(META_LOCATION_rp, 'rt')
    pointer = fh.readline()
    fh.close()
    pointer.strip()
    return int(pointer) - 1


def recover(recovery_param):
    """


    :rtype : last_successful_byte_marker
    :param topic:
    :param partition:
    :param offset:
    :return:
    """
    consumer_rebalance_listener = _CBL(recovery_param.topic, recovery_param.partition, recovery_param.offset)
    consumer.subscribe([recovery_param.topic], listener=consumer_rebalance_listener)

    while True:
        result_dict = consumer.poll(10)
        if result_dict:
            #tp = TopicPartition('ravitest', 0)
            tp = TopicPartition(recovery_param.topic, recovery_param.partition)
            consumer_record = result_dict[tp][-1]
            #consumer_record = result_dict[recovery_param.get_topic_partition()][-1]
            last_record = consumer_record[4]
            return last_record[:last_record.index(',')]
        else:
            pass

    return ''

if __name__ == "__main__":
    recovery_param = RecoveryParams('ravitest', 0, 1585247)
    last_successful_byte_marker = recover(recovery_param)
    print "Last successful Byte Marker : " + last_successful_byte_marker
