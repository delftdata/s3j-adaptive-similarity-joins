#!/usr/bin/env bash

k3s kubectl create namespace kafka
k3s kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
k3s kubectl apply -f deployments/kafka.yaml -n kafka