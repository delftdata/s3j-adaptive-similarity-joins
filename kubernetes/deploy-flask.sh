#!/usr/bin/env bash
eval $(minikube docker-env)
docker build -t coordinator ../coordinator
kubectl apply -f deployments/flask.yaml
eval $(minikube docker-env -u)