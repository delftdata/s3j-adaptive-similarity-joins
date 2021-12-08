#!/usr/bin/env bash
eval $(minikube docker-env)
docker build -t monitor ../monitor
kubectl apply -f deployments/monitor.yaml
eval $(minikube docker-env -u)