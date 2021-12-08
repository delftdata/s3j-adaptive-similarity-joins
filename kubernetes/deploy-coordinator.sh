#!/usr/bin/env bash
eval $(minikube docker-env)
docker build -t coordinator ../coordinator
kubectl apply -f deployments/coordinator.yaml
eval $(minikube docker-env -u)