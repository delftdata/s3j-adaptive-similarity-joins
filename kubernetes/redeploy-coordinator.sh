#!/usr/bin/env bash
source ./environment/dependencies.env
$KUBECTL delete deploy coordinator
$KUBECTL apply -f deployments/coordinator.yaml
