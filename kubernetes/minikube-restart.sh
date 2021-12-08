#!/bin/bash
minikube stop
minikube delete
minikube start --memory 12000 --cpus 16