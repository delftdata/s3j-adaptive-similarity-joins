#!/usr/bin/env bash

#./start-everything.sh


metrics=Flat_Map.numRecordsInPerSecond,Flat_Map.numRecordsOutPerSecond,Sink__Unnamed.KafkaProducer.record-send-rate
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
  printf '\nJobs started...\n'
  sleep 120
  printf '\nStarting generator...\n'
  curl -X POST -H "Content-Type: application/json" \
      -d "{\"args\": $generator_args}" \
      http://coordinator:5000/start_generator
  printf '\nStarting flink metrics monitoring...\n'
  python /Users/gsiachamis/Dropbox/"My Mac (Georgios’s MacBook Pro)"/Documents/GitHub/ssj-experiment-results/get_flink_metrics.py -en $name -om $metrics
  printf 'Experiment finished... \n'

  printf '\nReset experimental environment\n'
  curl http://coordinator:5000/reset_environment
  printf "\n\n"
  printf 'Reset kafka topics...\n'
  ./reset_kafka_topics.sh
  printf '\nEverything is reset!\n\n'
#  sleep 120

done < "$input"