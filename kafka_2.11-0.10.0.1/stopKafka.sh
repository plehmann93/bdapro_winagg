#!/bin/bash
rm -rf /tmp/kafka-logs/winagg-0/
./bin/kafka-server-stop.sh
./bin/zookeeper-server-stop.sh
echo "Done"
