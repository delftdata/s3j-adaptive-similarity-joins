#!/usr/bin/env bash

kubectl create namespace kafka
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
kubectl apply -f deployments/kafka.yaml -n kafka