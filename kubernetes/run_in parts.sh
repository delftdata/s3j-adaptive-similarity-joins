#!/usr/bin/env bash

kafka_bootstrap=$(kubectl get svc kafka-cluster-kafka-extern-bootstrap -n kafka --no-headers | awk '{print $4}')
input="$PWD/experiments.txt"

while IFS= read -r line
do
  printf 'Run experiment: %s\n' "$line"
  IFS=';' read -ra ss <<< "$line"
  name="${ss[0]}"
  ssj_args="${ss[1]}"
  generator_args="${ss[2]}"

  printf 'Setup experimental environment\n'
  curl http://coordinator:5000/setup

  printf '\nStart join job... \n'
  curl -X POST -H "Content-Type: application/json" -d "{\"args\": $ssj_args}" http://coordinator:5000/start
  printf '\nJob started...\n'
  sleep 60

  printf '\nStarting generator...\n'
  curl -X POST -H "Content-Type: application/json" \
      -d "{\"args\": $generator_args}" \
      http://coordinator:5000/start_generator

  printf '\nStarting flink metrics monitoring...\n'
  python /Users/gsiachamis/Dropbox/"My Mac (Georgios’s MacBook Pro)"/Documents/GitHub/ssj-experiment-results/get_flink_metrics.py -en "$name" -om $metrics
  printf 'Experiment finished... \n'

  printf '\nCalculating stats...\n'
  curl http://coordinator:5000/start_stats
  sleep 120
  printf '\nStats calculated\n'

  printf '\nCreating result plots...\n'
  offset="$(kubectl exec -i kafka-cluster-zookeeper-0 -n kafka -- ./bin/kafka-get-offsets.sh --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --topic pipeline-out-stats < /dev/null | awk -F':' '{print $3}')"
  python /Users/gsiachamis/Dropbox/"My Mac (Georgios’s MacBook Pro)"/Documents/GitHub/ssj-experiment-results/main.py -k "$kafka_bootstrap"":9094" -e "$offset" -n "$name"
  python /Users/gsiachamis/Dropbox/"My Mac (Georgios’s MacBook Pro)"/Documents/GitHub/ssj-experiment-results/draw.py -n "$name"
  printf '\nPlots are ready...\n'

done < "$input"

