#!/bin/bash
minikube stop
minikube delete
minikube start --memory 12000 --cpus 16
export GOOGLE_APPLICATION_CREDENTIALS=./credentials/application_default_credentials.json
minikube addons enable gcp-auth
