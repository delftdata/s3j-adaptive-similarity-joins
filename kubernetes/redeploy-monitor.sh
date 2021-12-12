#!/usr/bin/env bash
set -o allexport
source ./environment/dependencies.env
set +o allexport

$KUBECTL delete deploy monitor
$KUBECTL apply -f deployments/monitor.yaml
