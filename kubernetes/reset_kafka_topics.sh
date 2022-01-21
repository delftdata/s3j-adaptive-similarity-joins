#!/usr/bin/env bash
set -o allexport
source ./environment/dependencies.env
set +o allexport

$KUBECTL exec -n kafka -ti kafka-cluster-zookeeper-0 -- bin/kafka-topics.sh --delete --topic pipeline-in-left --bootstrap-server kafka-cluster-kafka-bootstrap:9092
$KUBECTL exec -n kafka -ti kafka-cluster-zookeeper-0 -- bin/kafka-topics.sh --delete --topic pipeline-in-right --bootstrap-server kafka-cluster-kafka-bootstrap:9092
$KUBECTL exec -n kafka -ti kafka-cluster-zookeeper-0 -- bin/kafka-topics.sh --delete --topic pipeline-out --bootstrap-server kafka-cluster-kafka-bootstrap:9092
$KUBECTL exec -n kafka -ti kafka-cluster-zookeeper-0 -- bin/kafka-topics.sh --delete --topic pipeline-out-stats --bootstrap-server kafka-cluster-kafka-bootstrap:9092

$KUBECTL exec -n kafka -ti kafka-cluster-zookeeper-0 -- bin/kafka-topics.sh --create --topic pipeline-in-left --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 1 --replication-factor 1
$KUBECTL exec -n kafka -ti kafka-cluster-zookeeper-0 -- bin/kafka-topics.sh --create --topic pipeline-in-right --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 1 --replication-factor 1
$KUBECTL exec -n kafka -ti kafka-cluster-zookeeper-0 -- bin/kafka-topics.sh --create --topic pipeline-out --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 1 --replication-factor 1
$KUBECTL exec -n kafka -ti kafka-cluster-zookeeper-0 -- bin/kafka-topics.sh --create --topic pipeline-out-stats --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 1 --replication-factor 1

