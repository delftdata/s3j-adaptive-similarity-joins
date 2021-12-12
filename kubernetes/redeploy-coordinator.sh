#!/usr/bin/env bash
set -o allexport
source ./environment/dependencies.env
set +o allexport

$KUBECTL delete deploy coordinator
$KUBECTL apply -f deployments/coordinator.yaml
