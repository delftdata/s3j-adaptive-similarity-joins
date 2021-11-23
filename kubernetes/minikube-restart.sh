#!/bin/bash
minikube stop
minikube delete
minikube start --memory 10000 --cpus 16