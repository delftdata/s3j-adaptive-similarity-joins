#!/usr/bin/env bash

kafka_bootstrap=$(kubectl get svc kafka-cluster-kafka-extern-bootstrap -n kafka --no-headers | awk '{print $4}')
input="$PWD/experiments.txt"
metrics=Co-Process-Broadcast-Keyed.numRecordsInPerSecond,Co-Process-Broadcast-Keyed.numRecordsOutPerSecond,Sink__Unnamed.KafkaProducer.record-send-rate

while IFS= read -r line
do
  printf 'Run experiment: %s\n' "$line"
  IFS=';' read -ra ss <<< "$line"
  name="${ss[0]}"
  ssj_args="${ss[1]}"
  generator_args="${ss[2]}"

  printf 'Setup experimental environment\n'
  curl http://coordinator:30080/setup

  printf '\nStart join job... \n'
  curl -X POST -H "Content-Type: application/json" -d "{\"args\": $ssj_args}" http://coordinator:30080/start
  printf '\nJob started...\n'
  sleep 60

  printf '\nStarting generator...\n'
  curl -X POST -H "Content-Type: application/json" \
      -d "{\"args\": $generator_args}" \
      http://coordinator:30080/start_generator

  printf '\nStarting flink metrics monitoring...\n'
  python ~/ssj-experiment-results/get_flink_metrics.py -en "$name" -om $metrics
  printf 'Experiment finished... \n'
  curl http://coordinator:30080/cancel_join_job

  printf '\nCalculating stats...\n'
  curl http://coordinator:30080/start_stats?parallelism=5
  sleep 20
  python ~/ssj-experiment-results/monitor_stats.py
  printf '\nStats calculated\n'

  printf '\nCreating result plots...\n'
  offset="$(kubectl exec -i kafka-cluster-zookeeper-0 -n kafka -- ./bin/kafka-get-offsets.sh --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --topic pipeline-out-stats < /dev/null | awk -F':' '{print $3}')"
  python ~/ssj-experiment-results/main.py -k "$kafka_bootstrap"":9094" -e "$offset" -n "$name" -l "/workspace/gsiachamis/ssj-results-debs/paper_run"
  python ~/ssj-experiment-results/draw.py -n "$name" -l "/workspace/gsiachamis/ssj-results-debs/paper_run"
  printf '\nPlots are ready...\n'

  printf '\nReset experimental environment\n'
  curl http://coordinator:30080/reset_environment
  printf "\n\n"
  printf 'Reset kafka topics...\n'
  ./reset_kafka_topics.sh < /dev/null
  printf '\nEverything is reset!\n\n'

done < "$input"

