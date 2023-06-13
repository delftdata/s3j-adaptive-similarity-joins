#!/usr/bin/env bash

for i in $(gcloud compute target-pools list --format='value(name)'); do
  yes | gcloud compute target-pools delete "$i"
done
