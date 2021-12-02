#!/bin/bash
# Quickstart: https://kafka.apache.org/quickstart
# Note that their way of adding topics is missing arguments (namely --partitions and --replica-factor), line below is the correct way
# You need to set KAFKA_HOME before calling this, uncommment the line below with the path to the kafka folder (replace the three dots)
#KAFKA_HOME=replacethis
$KAFKA_HOME/bin/kafka-topics.sh --create --topic $1 --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
