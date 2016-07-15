#!/bin/bash

topic=$1
partition=$2

ssh ntart@192.168.153.228 /usr/bin/kafka-run-class kafka.tools.GetOffsetShell --broker-list 192.168.153.227:9092 --topic iot0505 --partition 0 --time -1 | awk -F : '{print $3}' > ./meta/recovery_point