#!/usr/bin/env bash
source ./environment/dependencies.env
k3s kubectl port-forward service/minio 9000