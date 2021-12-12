#!/bin/bash
source ./environment/dependencies.env
STR=$(kubectl get deployments --all-namespaces | awk '{print $3}')
if [[ "$STR" == *"0"* ]]; then
  echo "Some deployments are not up yet..."
  k3s kubectl get deployments --all-namespaces
else
	echo "All green!"
fi
read -p "Press any key..."