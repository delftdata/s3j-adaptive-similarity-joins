#!/usr/bin/env bash
helm uninstall minio
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install --version v7.3.0 -f configs/minio-config.yaml minio bitnami/minio
