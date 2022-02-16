#!/usr/bin/env bash

kubectl delete deploy monitor
kubectl apply -f deployments/monitor.yaml
