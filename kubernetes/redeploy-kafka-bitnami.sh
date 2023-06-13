#!/usr/bin/env bash

kubectl delete namespace kafka
kubectl create namespace kafka
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install --namespace=kafka kafka bitnami/kafka -f deployments/kafka-bitnami.yaml
sleep 60
./reset_kafka_topics.sh
