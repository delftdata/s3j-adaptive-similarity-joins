#!/usr/bin/env bash

kubectl delete deploy coordinator
kubectl apply -f deployments/coordinator.yaml
