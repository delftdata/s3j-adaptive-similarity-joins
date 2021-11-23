#!/usr/bin/env bash
helm install --version v7.3.0 -f configs/minio-config.yaml minio bitnami/minio
