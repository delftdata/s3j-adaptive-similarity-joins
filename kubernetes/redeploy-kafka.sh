#!/usr/bin/env bash
source ./environment/dependencies.env
$KUBECTL delete namespace kafka
$KUBECTL create namespace kafka
$KUBECTL create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
$KUBECTL apply -f deployments/kafka.yaml -n kafka