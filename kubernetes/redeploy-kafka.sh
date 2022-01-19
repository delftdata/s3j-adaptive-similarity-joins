#!/usr/bin/env bash
set -o allexport
source ./environment/dependencies.env
set +o allexport

$KUBECTL delete namespace kafka
$KUBECTL create namespace kafka
helm repo add strimzi https://strimzi.io/charts/
helm uninstall strimzi-kafka
helm install --namespace kafka --version v0.27.1 strimzi-kafka strimzi/strimzi-kafka-operator
$KUBECTL apply -f deployments/kafka.yaml -n kafka

#Old deployment
#$KUBECTL delete namespace kafka
#$KUBECTL create namespace kafka
#$KUBECTL create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
#$KUBECTL apply -f deployments/kafka.yaml -n kafka