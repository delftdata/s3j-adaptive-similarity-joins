#!/bin/bash
set -o allexport
source ./environment/dependencies.env
set +o allexport

STR=$($KUBECTL get deployments --all-namespaces | awk '{print $3}')
if [[ "$STR" == *"0"* ]]; then
  echo "Some deployments are not up yet..."
  $KUBECTL get deployments --all-namespaces
else
	echo "All green!"
fi
read -p "Press any key..."