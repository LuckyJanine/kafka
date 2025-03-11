#!/bin/bash

LOOP_DEVICE="/home/ubuntu/kafka-vdisk/kafka-logs"
PORT=9100

while true; do
    AVAILABLE_SPACE=$(df --output=avail -B1 $LOOP_DEVICE | tail -1)

    # format output for prometheus
    METRICS="# HELP loop_device_available_space Available space on kafka-logs in bytes
# TYPE loop_device_available_space gauge
loop_device_available_space{device=\"$LOOP_DEVICE\"} $AVAILABLE_SPACE"

    { echo -e "HTTP/1.1 200 OK\nContent-Type: text/plain\n\n$METRICS"; } | nc -l -p $PORT -q 1
done