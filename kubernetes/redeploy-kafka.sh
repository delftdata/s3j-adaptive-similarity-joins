#!/usr/bin/env bash

kubectl delete namespace kafka
kubectl create namespace kafka
helm repo add strimzi https://strimzi.io/charts/
helm uninstall strimzi-kafka
helm install --namespace kafka --version v0.27.1 strimzi-kafka strimzi/strimzi-kafka-operator
kubectl apply -f deployments/kafka.yaml -n kafka
