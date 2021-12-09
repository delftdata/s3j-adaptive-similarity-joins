#!/bin/bash

kubectl create configmap env-config --from-env-file=./environment/.env

./deploy-minio.sh
./deploy-kafka.sh
./deploy-coordinator.sh
./deploy-monitor.sh

# Might need a wait loop for MinIO? Unless the service is up immediately
./deploy-flink.sh