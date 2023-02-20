#!/usr/bin/env bash

kafka_bootstrap="kafka"
input="$PWD/experiments_lb.txt"
metrics=Co-Process-Broadcast-Keyed.numRecordsInPerSecond,Co-Process-Broadcast-Keyed.numRecordsOutPerSecond,Sink__Unnamed.KafkaProducer.record-send-rate

while IFS= read -r line
do
  printf 'Run experiment: %s\n' "$line"
  IFS=';' read -ra ss <<< "$line"
  name="${ss[0]}"
  ssj_args="${ss[1]}"
  generator_args="${ss[2]}"
  generator_args_2="${ss[3]}"

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

  while true
  do
    sleep 10
    tmp="$(curl --silent http://coordinator:30080/jobs | grep -c 'RUNNING')"
    if (( tmp < 2 ))
    then
      printf '\nFirst generator finished\n'
      break
    fi
  done
  sleep 10

  curl http://coordinator:30080/cancel_join_job
  sleep 30
  kubectl exec -n kafka -i kafka-0 -- opt/bitnami/kafka/bin/kafka-topics.sh --delete --topic pipeline-in-left --bootstrap-server kafka:9092
  kubectl exec -n kafka -i kafka-0 -- opt/bitnami/kafka/bin/kafka-topics.sh --delete --topic pipeline-in-right --bootstrap-server kafka:9092
  kubectl exec -n kafka -i kafka-0 -- opt/bitnami/kafka/bin/kafka-topics.sh --create --topic pipeline-in-left --bootstrap-server kafka:9092 --partitions 15 --replication-factor 1
  kubectl exec -n kafka -i kafka-0 -- opt/bitnami/kafka/bin/kafka-topics.sh --create --topic pipeline-in-right --bootstrap-server kafka:9092 --partitions 15 --replication-factor 1

  printf "Restarting join job..."
  curl -X POST -H "Content-Type: application/json" -d "{\"args\": $ssj_args}" http://coordinator:30080/start
  printf '\nJob started...\n'
  sleep 60


  printf '\nStarting 2nd generator...\n'
  curl -X POST -H "Content-Type: application/json" \
      -d "{\"args\": $generator_args_2}" \
      http://coordinator:30080/start_generator

  while true
  do
    sleep 10
    tmp="$(curl --silent http://coordinator:30080/jobs | grep -c 'RUNNING')"
    if (( tmp < 2 ))
    then
      printf '\nFirst generator finished\n'
      break
    fi
  done
  sleep 10

  curl http://coordinator:30080/cancel_join_job
  sleep 30
  kubectl exec -n kafka -i kafka-0 -- opt/bitnami/kafka/bin/kafka-topics.sh --delete --topic pipeline-in-left --bootstrap-server kafka:9092
  kubectl exec -n kafka -i kafka-0 -- opt/bitnami/kafka/bin/kafka-topics.sh --delete --topic pipeline-in-right --bootstrap-server kafka:9092
  kubectl exec -n kafka -i kafka-0 -- opt/bitnami/kafka/bin/kafka-topics.sh --create --topic pipeline-in-left --bootstrap-server kafka:9092 --partitions 15 --replication-factor 1
  kubectl exec -n kafka -i kafka-0 -- opt/bitnami/kafka/bin/kafka-topics.sh --create --topic pipeline-in-right --bootstrap-server kafka:9092 --partitions 15 --replication-factor 1

  printf '\nCalculating stats...\n'
  curl http://coordinator:30080/start_stats?parallelism=5
  sleep 20
  python ~/ssj-experiment-results/monitor_stats.py
  printf '\nStats calculated\n'

  printf '\nCreating result plots...\n'
  offset="$(kubectl exec -i kafka-0 -n kafka -- opt/bitnami/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic pipeline-out-stats < /dev/null | awk -F':' '{print $3}')"
  python ~/ssj-experiment-results/main.py -k "$kafka_bootstrap"":30094" -e "$offset" -n "$name" -l "/workspace/gsiachamis/ssj-results-debs"
  python ~/ssj-experiment-results/draw.py -n "$name" -l "/workspace/gsiachamis/ssj-results-debs"
  printf '\nPlots are ready...\n'

  printf '\nReset experimental environment\n'
  curl http://coordinator:30080/reset_environment
  printf "\n\n"
  printf 'Reset kafka topics...\n'
  ./reset_kafka_topics.sh < /dev/null
  printf '\nEverything is reset!\n\n'

done < "$input"

