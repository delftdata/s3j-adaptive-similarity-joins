#!/usr/bin/env bash

kubectl delete deploy monitor
while [[ "$(kubectl get pods |(! grep "monitor"))" ]]; do
        echo "Pod still terminating... Waiting..."
        sleep 10
done

kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- bin/kafka-topics.sh --delete --topic pipeline-in-left --bootstrap-server kafka-cluster-kafka-bootstrap:9092
kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- bin/kafka-topics.sh --delete --topic pipeline-in-right --bootstrap-server kafka-cluster-kafka-bootstrap:9092
kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- bin/kafka-topics.sh --delete --topic pipeline-out --bootstrap-server kafka-cluster-kafka-bootstrap:9092
kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- bin/kafka-topics.sh --delete --topic pipeline-out-stats --bootstrap-server kafka-cluster-kafka-bootstrap:9092
kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- bin/kafka-topics.sh --delete --topic pipeline-side-out --bootstrap-server kafka-cluster-kafka-bootstrap:9092
kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- bin/kafka-topics.sh --delete --topic pipeline-throughput --bootstrap-server kafka-cluster-kafka-bootstrap:9092

kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- bin/kafka-topics.sh --create --topic pipeline-in-left --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 1 --replication-factor 1
kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- bin/kafka-topics.sh --create --topic pipeline-in-right --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 1 --replication-factor 1
kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- bin/kafka-topics.sh --create --topic pipeline-out --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 15 --replication-factor 1
kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- bin/kafka-topics.sh --create --topic pipeline-out-stats --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 1 --replication-factor 1
kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- bin/kafka-topics.sh --create --topic pipeline-side-out --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 15 --replication-factor 1
kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- bin/kafka-topics.sh --create --topic pipeline-throughput --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 15 --replication-factor 1



#kubectl apply -f deployments/monitor.yaml