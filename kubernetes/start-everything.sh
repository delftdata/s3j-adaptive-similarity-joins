#!/bin/bash
./minikube-restart.sh
./deploy-minio.sh
./deploy-kafka.sh
./deploy-flask.sh

# Might need a wait loop for MinIO? Unless the service is up immediately
./deploy-flink.sh

minikube dashboard