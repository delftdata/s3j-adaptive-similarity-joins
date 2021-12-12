#!/usr/bin/env bash
set -o allexport
source ./environment/dependencies.env
set +o allexport

$KUBECTL port-forward service/minio 9000