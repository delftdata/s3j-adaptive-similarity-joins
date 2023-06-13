import json
import os
import subprocess

from config import parseArguments
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from utils import update_per_window_dictionary
import time

stopped_job = ""
running_job_id = ""
global_group_sizes = {}
mlc_per_window = {}
glc_per_window = {}
ggs_per_window = {}
percentiles_per_window = {}
input_throughput_per_window = {}
inputSimJoin_per_sec = {}
processSimJoin_per_sec = {}
machines = []

type_to_dict = {
    "final-comps-per-machine": mlc_per_window,
    "final-comps-per-group": glc_per_window,
    "size-per-group": ggs_per_window,
    "latency-percentiles-per-machine": percentiles_per_window,
    "throughput": input_throughput_per_window
}

if __name__ == '__main__':
    args = parseArguments()
    # Connect to Kafka
    kafka_connected = False
    while not kafka_connected:
        try:
            print("Connecting to Kafka...")
            consumer = KafkaConsumer(
                bootstrap_servers=[args.kafka],
                auto_offset_reset='earliest',
            )
            kafka_connected = True
        except NoBrokersAvailable:
            print("Could not connect to Kafka, trying again in 10...")
            time.sleep(10)

    # Subscribe to the metrics topic.
    consumer.subscribe(topics=[args.topic])

    experiment_name = args.name
    if not os.path.isdir(os.path.join(args.location, "experiments", experiment_name, "stats_files")):
        os.makedirs(os.path.join(args.location, "experiments", experiment_name, "stats_files"))
    output_dir = os.path.join(args.location, "experiments", experiment_name,  "stats_files")

    print("Start reading input.")
    for item in consumer:
        # print("Received item: ", item)
        if item.key.decode() == "running_job_id":
            if running_job_id == "":
                running_job_id = item.value.decode()
                # print(running_job_id)
        elif item.key.decode() == "machines-id":
            machines = list(map(int, item.value.decode().split(",")))
            with open(output_dir+"/"+args.name+"__"+"machine_ids"+".txt", "w") as fh:
                for m in machines:
                    fh.write(str(m)+"\n")
            # print(machines)
            if not bool(global_group_sizes):
                global_group_sizes = dict.fromkeys(machines)
                for m in machines:
                    global_group_sizes[m] = dict()
        else:
            job_uuid, stats_key = item.key.decode().split("_")
            # print(job_uuid)
            if job_uuid != stopped_job:
                # print("Not stopped")
                if stats_key != "done":
                    # print("stats_key: ", stats_key)
                    item_window = eval(item.value.decode())["f0"]
                    if stats_key == "size-per-group":
                        # print(global_group_sizes)
                        update_per_window_dictionary(item, stats_key,
                                                     item_window,
                                                     type_to_dict[stats_key],
                                                     machines,
                                                     global_group_sizes)
                    elif stats_key == "throughput":
                        # print("throughput")
                        input_throughput_per_window[item_window] = eval(item.value.decode())["f1"]
                    else:
                        # print("Update with new value")
                        # print(item)
                        update_per_window_dictionary(item, stats_key, item_window, type_to_dict[stats_key], machines)

        if item.offset == int(args.end)-1:
            break

    for filename in type_to_dict.keys():
        with open(output_dir+"/"+args.name+"__"+filename+".txt", "w") as fh:
            fh.write(json.dumps(type_to_dict[filename]))

    if args.coordinator:
        bash_command = "kubectl logs %s | grep '\"POST /migrate HTTP/1.1\" 200' |" \
                       " grep -o '[0-2][0-9]:[0-5][0-9]:[0-5][0-9]'" \
                       "> %s" % (args.coordinator, "stats_files/"+args.name+"__migration.txt")
        subprocess.run(bash_command, shell=True)



