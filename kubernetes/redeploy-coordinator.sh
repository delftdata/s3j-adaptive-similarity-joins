#!/usr/bin/env bash

kubectl delete deploy coordinator
kubectl apply -f deployments/coordinator.yaml

while [[ "$(kubectl get svc coordinator --no-headers | awk '{if ($4=="<pending>" || $4=="<none>") print $4; else print "";}')" ]]; do
        echo "Waiting for Coordinator service external IP..."
        sleep 5
done

kubectl get svc coordinator --no-headers | awk '{print $4}'

sudo ./update_hostname.sh coordinator "$(kubectl get svc coordinator --no-headers | awk '{print $4}')"