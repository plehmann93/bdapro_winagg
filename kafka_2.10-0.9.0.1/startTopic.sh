#!/bin/bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic winagg
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic winagg < nyc200000
