#!/bin/bash

kubectl delete configmap env-config
kubectl create configmap env-config --from-env-file=./environment/.env

./redeploy-minio.sh
./redeploy-kafka.sh
./redeploy-coordinator.sh
#./redeploy-monitor.sh

while [[ -z "$(kubectl get svc minio | awk '{print $3}')" ]]; do
	echo 'MinIO service has no cluster IP yet, waiting...'
	sleep 1
done
./redeploy-flink.sh