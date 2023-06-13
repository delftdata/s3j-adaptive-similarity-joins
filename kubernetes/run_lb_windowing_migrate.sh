#!/usr/bin/env bash

kafka_bootstrap=$(kubectl get svc kafka-cluster-kafka-extern-bootstrap -n kafka --no-headers | awk '{print $4}')
input="$PWD/experiments_lb.txt"
metrics=Co-Process-Broadcast-Keyed.numRecordsInPerSecond,Co-Process-Broadcast-Keyed.numRecordsOutPerSecond,Sink__Unnamed.KafkaProducer.record-send-rate

while IFS= read -r line
do
  printf 'Run experiment: %s\n' "$line"
  IFS=';' read -ra ss <<< "$line"
  name="${ss[0]}"
  ssj_args="${ss[1]}"
  generator_args="${ss[2]}"
  generator_args_2="${ss[3]}"", -seed, 70, uniform_MD_generator, uniform_MD_generator\""
  generator_args_3="${ss[3]}"", -seed, 150, uniform_MD_generator, uniform_MD_generator\""
  generator_args_4="${ss[3]}"", -seed, 90, uniform_MD_generator, uniform_MD_generator\""
  generator_args_5="${ss[3]}"", -seed, 120, uniform_MD_generator, uniform_MD_generator\""


  printf 'Setup experimental environment\n'
  curl http://coordinator:30080/setup

  printf '\nStart join job... \n'
  curl -X POST -H "Content-Type: application/json" -d "{\"args\": $ssj_args}" http://coordinator:30080/start
  printf '\nJob started...\n'
  sleep 60
  join_job=$(curl http://coordinator:30080/get_join_jobid)
  echo $join_job

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
  
  curl -X POST -H "Content-Type: application/json" -d "{\"job_id_to_stop\": \"$join_job\"}" http://coordinator:30080/stop

  while true
  do
    sleep 30
    tmp="$(curl --silent http://coordinator:30080/jobs | grep -c 'RUNNING')"
    echo $tmp
    if (( tmp > 0 ))
    then
      printf '\nWaiting for job to stop...\n'
    else
      break
    fi
  done

  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --delete --topic pipeline-in-left --bootstrap-server kafka-cluster-kafka-bootstrap:9092
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --delete --topic pipeline-in-right --bootstrap-server kafka-cluster-kafka-bootstrap:9092
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --create --topic pipeline-in-left --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 15 --replication-factor 1
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --create --topic pipeline-in-right --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 15 --replication-factor 1

  printf '\nCalculating stats...\n'
  curl http://coordinator:30080/start_stats?parallelism=1
  sleep 20
  python ~/ssj-experiment-results/monitor_stats.py
  printf '\nStats calculated\n'
  sleep 120
  curl http://coordinator:30080/cancel_stats_job

  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --delete --topic pipeline-out --bootstrap-server kafka-cluster-kafka-bootstrap:9092
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --delete --topic pipeline-side-out --bootstrap-server kafka-cluster-kafka-bootstrap:9092
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --create --topic pipeline-out --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 15 --replication-factor 1
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --create --topic pipeline-side-out --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 15 --replication-factor 1

  kubectl apply -f deployments/monitor.yaml

  while true
  do
    sleep 10
    tmp="$(curl --silent http://coordinator:30080/jobs | grep -c 'RUNNING')"
    if (( tmp < 1 ))
    then
      printf '\nWaiting for job to restart...\n'
    else  
      break
    fi
  done
  kubectl delete deploy monitor
  sleep 60
  join_job=$(curl http://coordinator:30080/get_join_jobid)

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
      printf '\nSecond generator finished\n'
      break
    fi
  done

  curl -X POST -H "Content-Type: application/json" -d "{\"job_id_to_stop\": \"$join_job\"}" http://coordinator:30080/stop

  while true
  do
    sleep 30
    tmp="$(curl --silent http://coordinator:30080/jobs | grep -c 'RUNNING')"
    if (( tmp > 0 ))
    then
      printf '\nWaiting for job to stop...\n'
    else
      break
    fi
  done
  
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --delete --topic pipeline-in-left --bootstrap-server kafka-cluster-kafka-bootstrap:9092
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --delete --topic pipeline-in-right --bootstrap-server kafka-cluster-kafka-bootstrap:9092
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --create --topic pipeline-in-left --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 15 --replication-factor 1
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --create --topic pipeline-in-right --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 15 --replication-factor 1

  printf '\nCalculating stats...\n'
  curl http://coordinator:30080/start_stats?parallelism=1
  sleep 20
  python ~/ssj-experiment-results/monitor_stats.py
  printf '\nStats calculated\n'
  sleep 120
  curl http://coordinator:30080/cancel_stats_job

  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --delete --topic pipeline-out --bootstrap-server kafka-cluster-kafka-bootstrap:9092
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --delete --topic pipeline-side-out --bootstrap-server kafka-cluster-kafka-bootstrap:9092
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --create --topic pipeline-out --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 15 --replication-factor 1
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --create --topic pipeline-side-out --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 15 --replication-factor 1

  kubectl apply -f deployments/monitor.yaml
  
  while true
  do
    sleep 10
    tmp="$(curl --silent http://coordinator:30080/jobs | grep -c 'RUNNING')"
    if (( tmp < 1 ))
    then
      printf '\nWaiting for job to restart...\n'
    else  
      break
    fi
  done
  kubectl delete deploy monitor
  sleep 60
  join_job=$(curl http://coordinator:30080/get_join_jobid)

  printf '\nStarting 3rd generator...\n'
  curl -X POST -H "Content-Type: application/json" \
      -d "{\"args\": $generator_args_3}" \
      http://coordinator:30080/start_generator

  while true
  do
    sleep 10
    tmp="$(curl --silent http://coordinator:30080/jobs | grep -c 'RUNNING')"
    if (( tmp < 2 ))
    then
      printf '\nThird generator finished\n'
      break
    fi
  done
  sleep 30

  curl -X POST -H "Content-Type: application/json" -d "{\"job_id_to_stop\": \"$join_job\"}" http://coordinator:30080/stop
  while true
  do
    sleep 30
    tmp="$(curl --silent http://coordinator:30080/jobs | grep -c 'RUNNING')"
    if (( tmp > 0 ))
    then
      printf '\nWaiting for job to stop...\n'
    else  
      break
    fi
  done
  
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --delete --topic pipeline-in-left --bootstrap-server kafka-cluster-kafka-bootstrap:9092
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --delete --topic pipeline-in-right --bootstrap-server kafka-cluster-kafka-bootstrap:9092
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --create --topic pipeline-in-left --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 15 --replication-factor 1
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --create --topic pipeline-in-right --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 15 --replication-factor 1

  printf '\nCalculating stats...\n'
  curl http://coordinator:30080/start_stats?parallelism=1
  sleep 20
  python ~/ssj-experiment-results/monitor_stats.py
  printf '\nStats calculated\n'
  sleep 120
  curl http://coordinator:30080/cancel_stats_job

  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --delete --topic pipeline-out --bootstrap-server kafka-cluster-kafka-bootstrap:9092
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --delete --topic pipeline-side-out --bootstrap-server kafka-cluster-kafka-bootstrap:9092
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --create --topic pipeline-out --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 15 --replication-factor 1
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --create --topic pipeline-side-out --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 15 --replication-factor 1

  kubectl apply -f deployments/monitor.yaml
  
  while true
  do
    sleep 10
    tmp="$(curl --silent http://coordinator:30080/jobs | grep -c 'RUNNING')"
    if (( tmp < 1 ))
    then
      printf '\nWaiting for job to restart...\n'
    else  
      break
    fi
  done
  kubectl delete deploy monitor
  sleep 60
  join_job=$(curl http://coordinator:30080/get_join_jobid)

  printf '\nStarting 4th generator...\n'
  curl -X POST -H "Content-Type: application/json" \
      -d "{\"args\": $generator_args_4}" \
      http://coordinator:30080/start_generator

  while true
  do
    sleep 10
    tmp="$(curl --silent http://coordinator:30080/jobs | grep -c 'RUNNING')"
    if (( tmp < 2 ))
    then
      printf '\nFourth generator finished\n'
      break
    fi
  done
  curl -X POST -H "Content-Type: application/json" -d "{\"job_id_to_stop\": \"$join_job\"}" http://coordinator:30080/stop
  while true
  do
    sleep 30
    tmp="$(curl --silent http://coordinator:30080/jobs | grep -c 'RUNNING')"
    if (( tmp > 0 ))
    then
      printf '\nWaiting for job to stop...\n'
    else  
      break
    fi
  done
  
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --delete --topic pipeline-in-left --bootstrap-server kafka-cluster-kafka-bootstrap:9092
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --delete --topic pipeline-in-right --bootstrap-server kafka-cluster-kafka-bootstrap:9092
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --create --topic pipeline-in-left --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 15 --replication-factor 1
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --create --topic pipeline-in-right --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 15 --replication-factor 1

  printf '\nCalculating stats...\n'
  curl http://coordinator:30080/start_stats?parallelism=1
  sleep 20
  python ~/ssj-experiment-results/monitor_stats.py
  printf '\nStats calculated\n'
  sleep 120
  curl http://coordinator:30080/cancel_stats_job

  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --delete --topic pipeline-out --bootstrap-server kafka-cluster-kafka-bootstrap:9092
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --delete --topic pipeline-side-out --bootstrap-server kafka-cluster-kafka-bootstrap:9092
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --create --topic pipeline-out --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 15 --replication-factor 1
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --create --topic pipeline-side-out --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 15 --replication-factor 1

  kubectl apply -f deployments/monitor.yaml
  
  while true
  do
    sleep 10
    tmp="$(curl --silent http://coordinator:30080/jobs | grep -c 'RUNNING')"
    if (( tmp < 1 ))
    then
      printf '\nWaiting for job to restart...\n'
    else  
      break
    fi
  done
  kubectl delete deploy monitor
  sleep 60
  join_job=$(curl http://coordinator:30080/get_join_jobid)

  printf '\nStarting 5th generator...\n'
  curl -X POST -H "Content-Type: application/json" \
      -d "{\"args\": $generator_args_5}" \
      http://coordinator:30080/start_generator

  while true
  do
    sleep 10
    tmp="$(curl --silent http://coordinator:30080/jobs | grep -c 'RUNNING')"
    if (( tmp < 2 ))
    then
      printf '\nFifth generator finished\n'
      break
    fi
  done
  sleep 30

  curl http://coordinator:30080/cancel_join_job
  sleep 30
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --delete --topic pipeline-in-left --bootstrap-server kafka-cluster-kafka-bootstrap:9092
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --delete --topic pipeline-in-right --bootstrap-server kafka-cluster-kafka-bootstrap:9092
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --create --topic pipeline-in-left --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 15 --replication-factor 1
  kubectl exec -n kafka -i kafka-cluster-zookeeper-0 -- ./bin/kafka-topics.sh --create --topic pipeline-in-right --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --partitions 15 --replication-factor 1


  printf '\nCalculating stats...\n'
  curl http://coordinator:30080/start_stats?parallelism=1
  sleep 20
  python ~/ssj-experiment-results/monitor_stats.py
  printf '\nStats calculated\n'
  sleep 120

  printf '\nCreating result plots...\n'
  offset="$(kubectl exec -i kafka-cluster-zookeeper-0 -n kafka -- ./bin/kafka-get-offsets.sh --bootstrap-server kafka-cluster-kafka-bootstrap:9092 --topic pipeline-out-stats < /dev/null | awk -F':' '{print $3}')"
  python ~/ssj-experiment-results/main.py -k "$kafka_bootstrap"":9094" -e "$offset" -n "$name" -l "/workspace/gsiachamis/ssj-results-debs/lb_run"
  python ~/ssj-experiment-results/draw.py -n "$name" -l "/workspace/gsiachamis/ssj-results-debs/lb_run"
  printf '\nPlots are ready...\n'

  printf '\nReset experimental environment\n'
  curl http://coordinator:30080/reset_environment
  printf "\n\n"
  printf 'Reset kafka topics...\n'
  ./reset_kafka_topics.sh < /dev/null
  printf '\nEverything is reset!\n\n'

done < "$input"

