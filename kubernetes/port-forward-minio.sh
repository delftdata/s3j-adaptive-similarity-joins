#!/usr/bin/env bash
source ./environment/dependencies.env
$KUBECTL port-forward service/minio 9000