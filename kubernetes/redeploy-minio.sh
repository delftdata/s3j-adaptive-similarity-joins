#!/usr/bin/env bash
source ./environment/dependencies.env
helm uninstall minio
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install --version v7.3.0 -f configs/minio-config.yaml minio bitnami/minio
