import copy

from deserializing import deserialize_percentiles, deserialize


def update_sizes(gs: dict, ws: dict):
    for m in gs.keys():
        for key in ws.get(m).keys():
            if key in gs[m].keys():
                gs[m][key] = gs[m][key] + ws[m][key]
            else:
                gs[m][key] = ws[m][key]

##################################


def update_per_window_dictionary(item, stats_type: str, window: int, dictionary: dict, machines: list, ggs=None):

    decoded_values = eval(item.value.decode())

    if(stats_type == "latency-percentiles-per-machine"):
        deserialized_values = deserialize_percentiles(decoded_values, machines)
    else:
        deserialized_values = deserialize(decoded_values, machines)

    if(stats_type == "size-per-group"):
        update_sizes(ggs, deserialized_values)
        dictionary[window] = copy.deepcopy(ggs)
    else:
        dictionary[window] = deserialized_values

##################################
