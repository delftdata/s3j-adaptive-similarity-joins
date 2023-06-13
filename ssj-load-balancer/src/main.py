import io
import json
import time
import os

import minio
import random as rnd

import requests
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from setup import setup
from deserializing import deserialize, deserialize_latency_tuple, deserialize_percentiles
from latency import AverageLatency, LatencyMonitoring, get_average_latency
from load_rebalancing import Rebalancer, format_lb_input
from utils import update_sizes

machines = [0, 13, 26, 39, 52, 65, 78, 91, 104, 117]

group_keys = [(1, 0, 0), (2, 0, 0), (2, 13, 13), (1, 13, 13), (1, 26, 26), (2, 26, 26), (1, 39, 39), (3, 39, 39),
              (2, 39, 39), (2, 52, 52), (1, 52, 52), (1, 65, 65), (2, 65, 65), (2, 78, 78), (3, 78, 78), (1, 78, 78),
              (1, 91, 91), (1, 104, 104), (2, 104, 104), (1, 117, 117), (2, 117, 117)]

# flags
machine_level_computations_flag = False
group_level_computations_flag = False
group_level_size_flag = False
metric_flag = False
machine_level_computations_info = None
group_level_computations_info = None
group_level_size_info = None
metric_info = None
stopped_job = ""
running_job_id = ""
current_window = 0
global_group_sizes = {}


def print_average_latencies(average_latencies):
    printable = dict()
    for m in average_latencies.keys():
        printable[m] = get_average_latency(average_latencies[m][0], average_latencies[m][1])
    print(printable)


def create_dummy_example():
    rnd.seed(42)
    load_rebalancing_input = dict.fromkeys(machines)
    for key in machines:
        load_rebalancing_input[key] = []
    for key in group_keys:
        load = rnd.randint(0, 100)
        migration_cost = rnd.randint(0, 200)
        load_rebalancing_input[key[1]].append((key, load, migration_cost))

    load_sum = 0
    count = 0
    for key in load_rebalancing_input.keys():
        for group in load_rebalancing_input[key]:
            load_sum += group[1]
        count += 1
    average = load_sum/count

    return load_rebalancing_input, average


if __name__ == "__main__":

    # Read cmd args and configuration file
    args, config = setup()
    coordinator_location = os.environ['COORDINATOR_ADDRESS']

    # Setup HDFS and Kafka Clients
    kafka_connected = False
    while not kafka_connected:
        try:
            print("Connecting to Kafka...")
            consumer = KafkaConsumer(
                bootstrap_servers=[os.environ['KAFKA_ADDRESS']],
                auto_offset_reset='earliest',
                group_id=None
            )
            kafka_connected = True
        except NoBrokersAvailable:
            print("Could not connect to Kafka, trying again in 10...")
            time.sleep(10)

    consumer.subscribe(topics=[config["Kafka"]["topic.stats"]])
    minio_client = minio.Minio(
        os.environ["MINIO_ADDRESS"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False
    )
    if not minio_client.bucket_exists(os.environ["ALLOC_BUCKET_NAME"]):
        minio_client.make_bucket(os.environ["ALLOC_BUCKET_NAME"])

    # Monitoring & Statistics


    constraints = {"percentage": 0.2}
    latency_monitor = LatencyMonitoring(constraints)

    if args.all:
        algorithms = ["aaa", "aaa_m", "mr-partition", "greedy", "imlp_m", "imlp_c"]
    else:
        algorithms = args.algorithms

    rebalancer = Rebalancer(algorithms, args.minimize, config)

    print("Waiting input...\n")
    for item in consumer:
        print("Received item: ", item)
        if item.key.decode() == "running_job_id":
            if running_job_id == "":
                running_job_id = item.value.decode()
        elif item.key.decode() == "machines-id":
            machines = list(map(int, item.value.decode().split(",")))
            print(machines)
            if not bool(global_group_sizes):
                global_group_sizes = dict.fromkeys(machines)
                for m in machines:
                    global_group_sizes[m] = dict()
        else:
            job_uuid, stats_key = item.key.decode().split("_")
            print(stopped_job)
            if job_uuid != stopped_job:
                item_window = eval(item.value.decode())["f0"]
                if item_window > current_window:
                    machine_level_computations_flag = False
                    group_level_computations_flag = False
                    group_level_size_flag = False
                    metric_flag = False
                    current_window = item_window
                if item_window < current_window:
                    pass
                else:
                    print("setting flags")
                    if stats_key == "final-comps-per-machine":
                        machine_level_computations_flag = True
                        machine_level_computations_info = eval(item.value.decode())
                    elif stats_key == "final-comps-per-group":
                        group_level_computations_flag = True
                        group_level_computations_info = eval(item.value.decode())
                    elif stats_key == "size-per-group":
                        group_level_size_flag = True
                        group_level_size_info = eval(item.value.decode())
                    elif stats_key == "av-latency-per-machine" and args.average:
                        metric_flag = True
                        metric_info = eval(item.value.decode())
                    elif stats_key == "latency-percentiles-per-machine" and not args.average:
                        metric_flag = True
                        metric_info = eval(item.value.decode())
                print("machine_level_computations_flag: ", machine_level_computations_flag)
                print("group_level_computations_flag: ", group_level_computations_flag)
                print("group_level_size_flag: ", group_level_size_flag)
                print("metric_flag: ", metric_flag)
                print("current_window: ", current_window)
                if (machine_level_computations_flag
                        and group_level_computations_flag
                        and group_level_size_flag
                        and metric_flag
                ):
                    print("into deserialization")
                    machine_computations = deserialize(machine_level_computations_info, machines)
                    group_computations = deserialize(group_level_computations_info, machines)
                    window_group_sizes = deserialize(group_level_size_info, machines)
                    if args.average:
                        metric_values = deserialize(metric_info, machines)
                    else:
                        metric_values = deserialize_percentiles(metric_info, machines)

                    print(metric_values)

                    update_sizes(global_group_sizes, window_group_sizes)

                    if latency_monitor.check_latency_constraints(metric_values, args):

                        # ==================================================================================================
                        # Load rebalancing                        
                        # old_assignment, average = create_dummy_example()
                        old_assignment, average = \
                            format_lb_input(
                                group_computations,
                                global_group_sizes,
                                machine_computations
                            )

                        algo, result = rebalancer.create_new_assignment(old_assignment, average)
                        print("algorithm: %s, load_imbalance: %d, migration: %d, runtime %f" %
                                (algo, result.load_imbalance, result.migration, result.runtime))

                        group_2_node_dict = dict()
                        for node in result.assignment.keys():
                            for group in result.assignment.get(node):
                                group_2_node_dict[str(group[0])] = node

                        filename = time.strftime("%Y%m%d-%H%M%S")+".txt"
                        alloc = json.dumps(group_2_node_dict)
                        # print(alloc)
                        minio_client.put_object(os.environ["ALLOC_BUCKET_NAME"],
                                                filename,
                                                data=io.BytesIO(alloc.encode('utf-8')),
                                                length=len(alloc))

                        r = requests.post("http://" + coordinator_location+"/migrate", json={'filename': filename})
                        # print(r)
                        running_job_id = r.json()["jobid"]

                        machine_level_computations_flag = False
                        group_level_computations_flag = False
                        group_level_size_flag = False
                        metric_flag = False

                    # ==================================================================================================

                    else:
                        machine_level_computations_flag = False
                        group_level_computations_flag = False
                        group_level_size_flag = False
                        metric_flag = False
# TODO:
#   - Fix MR-Partition: Improve the optimal estimation or terminate earlier/assign arbitrarily the extra big groups.
#   - Fix IMLP: penalization and its indices.
