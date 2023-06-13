import json
import time

import requests

if __name__ == "__main__":

    metrics = "Sink__Unnamed.KafkaProducer.record-send-rate"
    source_metrics = {"Source__SSJ-Kafka-Output": "Source__SSJ-Kafka-Output.KafkaSourceReader.KafkaConsumer.bytes-consumed-rate",
                      "Source__SSJ-Kafka-Side-Output": "Source__SSJ-Kafka-Side-Output.KafkaSourceReader.KafkaConsumer.bytes-consumed-rate"}

    cj_stats_jobid_request = requests.get(f'http://coordinator:30080/get_cj_stats_id')
    cj_stats_id = cj_stats_jobid_request.text

    r = requests.get(f'http://flink-rest:30080/jobs/{cj_stats_id}/plan')
    # print(json.dumps(r.json(), indent=4))
    vertices = []
    source = {}
    for node in r.json()["plan"]["nodes"]:
        if "Sink" in node["description"]:
            vertices.append(node["id"])
        if "SSJ-Kafka-Output" in node["description"]:
            source[node["id"]] = source_metrics["Source__SSJ-Kafka-Output"]
        if "SSJ-Kafka-Side-Output" in node["description"]:
            source[node["id"]] = source_metrics["Source__SSJ-Kafka-Side-Output"]
    # print(source)


    # print(json.dumps(r.json(), indent=4))
    # print(vertices)

    while 1:
        zero = True
        for vertex in vertices:
            b = requests.get(f'http://flink-rest:30080/jobs/{cj_stats_id}/vertices/{vertex}/subtasks/metrics'
                             f'?get={metrics}')
            # print("sink requests\n")
            # print(json.dumps(b.json(), indent=4))
            if b.json():
                if b.json()[0]["sum"] != 0.0:
                    zero = False
            else:
                zero = False
        for s in source.keys():
            b = requests.get(f'http://flink-rest:30080/jobs/{cj_stats_id}/vertices/{s}/subtasks/metrics'
                             f'?get={source[s]}')
            # print("sources requests\n")
            # print(json.dumps(b.json(), indent=4))
            if b.json():
                if b.json()[0]["sum"] != 0.0:
                    zero = False
            else:
                zero = False
        if zero:
            break
        time.sleep(5)
