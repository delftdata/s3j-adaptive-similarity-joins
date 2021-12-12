#!/usr/bin/env bash
source ./environment/dependencies.env
$KUBECTL delete deploy monitor
$KUBECTL apply -f deployments/monitor.yaml
