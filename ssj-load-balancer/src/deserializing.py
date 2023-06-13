
def deserialize(kafka_input, machines):
    deserialized_dict = dict.fromkeys(machines)

    if kafka_input.get("f1")[0]["arity"] == 4:
        for m in machines:
            deserialized_dict[m] = dict()
        for t in kafka_input.get("f1"):
            machine_id, space_id, group_id, value, _ = t.values()
            deserialized_dict[machine_id][(group_id, machine_id, space_id)] = value
    elif kafka_input.get("f1")[0]["arity"] == 2:
        for t in kafka_input.get("f1"):
            machine_id, value, _ = t.values()
            deserialized_dict[machine_id] = value
    elif kafka_input.get("f1")[0]["arity"] == 3:
        for t in kafka_input.get("f1"):
            machine_id, sum, count, _ = t.values()
            deserialized_dict[machine_id] = (sum, count)

    return deserialized_dict


def deserialize_percentiles(kafka_input, machines):
    deserialized_dict = dict.fromkeys(machines)
    for t in kafka_input.get("f1"):
        machine_id, value, _ = t.values()
        tmp = dict()
        for p in value:
            key, value, _ = p.values()
            tmp[key] = value
        deserialized_dict[machine_id] = tmp
    return deserialized_dict


def deserialize_latency_tuple(kafka_tuple: dict):
    output_processing_time, input_processing_time, left_id, right_id, machine_id, _ = kafka_tuple.values()
    return output_processing_time, input_processing_time, left_id, right_id, machine_id


