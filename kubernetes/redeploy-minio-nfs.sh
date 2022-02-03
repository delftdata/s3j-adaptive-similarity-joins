#!/usr/bin/env bash
set -o allexport
source ./environment/dependencies.env
set +o allexport

helm uninstall minio
$KUBECTL delete pvc data-0-minio-0
$KUBECTL delete pvc data-0-minio-1
$KUBECTL delete pvc data-1-minio-0
$KUBECTL delete pvc data-1-minio-1
helm repo add bitnami https://charts.bitnami.com/bitnami
$KUBECTL create -f persistent-volumes/minio-nfs-pv.yaml
helm install --version v7.3.0 -f configs/minio-config.yaml minio bitnami/minio
