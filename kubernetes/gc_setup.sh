#!/usr/bin/env bash

# define project if multiple projects
# gcloud config set project psyched-metrics-242611

REGION=europe-west4
ZONE=${REGION}-a
PROJECT=$(gcloud config get-value project)
CLUSTER=ssj-cluster
# SCOPE="https://www.googleapis.com/auth/cloud-platform"

gcloud config set compute/zone ${ZONE}
gcloud config set project ${PROJECT}

# budget cluster
gcloud container clusters create $CLUSTER \
   --zone $ZONE \
   --machine-type "e2-standard-8" \
   --num-nodes=3 \
   --disk-size=100
   # --scopes $SCOPE \

# bigger cluster
gcloud container clusters create $CLUSTER \
   --zone $ZONE \
   --machine-type "e2-standard-16" \
   --num-nodes=3 \
   --disk-size=30

# to get kubectl
gcloud container clusters get-credentials $CLUSTER \
   --zone $ZONE \
   --project $PROJECT

gcloud container clusters delete $CLUSTER --zone $ZONE