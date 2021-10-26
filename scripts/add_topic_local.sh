#!/bin/bash
${KAFKA_HOME}/bin/kafka-topics.sh --create --topic my-topic --bootstrap-server localhost:9092 --partitions 1 --replica-factor 1
