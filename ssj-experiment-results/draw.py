import itertools
import json
import os
import random
from datetime import datetime

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from config import parseDrawArguments

percent_keys = ["50%", "90%", "95%", "99%", "99.9%"]
machines = ["0", "13", "26", "39", "52", "64", "77", "90", "103", "116"]

filenames = [
    "final-comps-per-machine",
    "final-comps-per-group",
    "size-per-group",
    "latency-percentiles-per-machine"]


def draw_computations_all_machines(df: pd.DataFrame, filename: str):
    pl = df.plot("Windows", df.columns[:-1], ylabel="Number of computations", title="Final Computations")
    plt.legend(bbox_to_anchor=(1.01, 1.0))
    plt.savefig(filename+"comps_per_machine.png", bbox_inches="tight")
    plt.close()


def draw_all_percentiles_per_machine(df: pd.DataFrame, machine: str, filename: str):
    new_df = df.loc[:, [machine, "Windows"]]
    new_df.loc[:, (machine, df.columns.get_level_values(1).drop_duplicates()[:-1])] =\
        new_df.loc[:, (machine, df.columns.get_level_values(1).drop_duplicates()[:-1])].div(1000)
    new_df.columns = new_df.columns.rename('Machine', level=0)
    new_df.columns = new_df.columns.rename('Percentile', level=1)
    pl = new_df.plot("Windows", new_df.columns.get_level_values(0).drop_duplicates()[:-1],
                     ylabel="Latency(s)")
    plt.legend(bbox_to_anchor=(1.01, 1.0))
    plt.savefig(filename+'machine_%s-all_percentiles.png' % machine, bbox_inches="tight")
    plt.close()


def draw_percentiles_line_chart(df: pd.DataFrame, perc : str, filename: str):
    new_df = df.loc[:, (df.columns.get_level_values(0).drop_duplicates(), ["", perc])]
    new_df.loc[:, (df.columns.get_level_values(0).drop_duplicates()[:-1], perc)] =\
        new_df.loc[:, (df.columns.get_level_values(0).drop_duplicates()[:-1], perc)].div(1000)
    new_df.columns = new_df.columns.rename('Machines', level=0)
    new_df.columns = new_df.columns.rename('Percentile', level=1)
    pl = new_df.plot("Windows", new_df.columns.get_level_values(0).drop_duplicates()[:-1],
                     ylabel="Latency(s)")
    # for i, p in enumerate(new_df["0"][perc]):
    #     plt.text(i, p+20, "%.2f" %p, ha="center", color="magenta")
    plt.legend(bbox_to_anchor=(1.01, 1.0))
    plt.savefig(filename+'%s-percentile.png' % perc, bbox_inches="tight")
    plt.close()


def draw_input_throughput(df: pd.DataFrame, filename: str):
    pl = df.plot("Timestamps", "Throughput", ylabel="Input Throughput(records/sec)")
    plt.savefig(filename+"input-throughput", bbox_inches="tight")
    plt.close()


def fill_nones(js: dict, percentiles: list):
    for t in js.keys():
        for m in js[t].keys():
            if js[t][m] is None:
                js[t][m] = dict.fromkeys(percentiles, 0)


def percentiles_dataframe(js: dict):
    global percent_keys, machines
    timestamps = []
    percentiles = []
    fill_nones(js, percent_keys)
    for _ in range(len(percent_keys)*len(machines)):
        percentiles.append([])
    for timestamp in js.keys():
        timestamps.append(timestamp)
    for t in timestamps:
        for m_i, m in enumerate(machines):
            for p_i, p in enumerate(percent_keys):
                percentiles[len(percent_keys)*m_i + p_i].append(js[t][m][p])
    np_percentiles = np.array(percentiles)
    df = pd.DataFrame(data=np_percentiles.T, columns=pd.MultiIndex.from_tuples(itertools.product(machines,percent_keys)))
    windows = [datetime.utcfromtimestamp(int(timestamp) / 1000).strftime("%H:%M:%S") for timestamp in timestamps]
    # windows = [idx for idx, _ in enumerate(timestamps)]
    df["Windows"] = np.array(windows).T
    df.sort_values(by="Windows", inplace=True)
    return df


def machine_computations_dataframe(js: dict):
    global machines
    timestamps = []
    computations = []
    for _ in range(len(machines)):
        computations.append([])
    for timestamp in js.keys():
        timestamps.append(timestamp)
    for t in timestamps:
        for m_i, m in enumerate(machines):
            computations[m_i].append(js[t][m])
    np_computations = np.array(computations)
    df = pd.DataFrame(data=np_computations.T, columns=machines)
    windows = [datetime.utcfromtimestamp(int(timestamp) / 1000).strftime("%H:%M:%S") for timestamp in timestamps]
    df["Windows"] = np.array(windows).T
    df.sort_values(by="Windows", inplace=True)
    return df


def group_size_dataframe(js: dict):
    global machines
    timestamps = []
    groups = []
    sizes = []
    for t in js.keys():
        timestamps.append(t)
        for m in machines:
            for g in js[t][m].keys():
                groups.append((m,g))
    for m, g in groups:
        tmp = []
        for t in timestamps:
            if g in js[t][m].keys():
                tmp.append(js[t][m][g])
            else:
                tmp.append(None)
        sizes.append(tmp)
    np_sizes = np.array(sizes)
    df = pd.DataFrame(data=np_sizes.T, columns=pd.MultiIndex.from_tuples(groups))
    windows = [datetime.utcfromtimestamp(int(timestamp) / 1000).strftime("%H:%M:%S") for timestamp in timestamps]
    df["Windows"] = np.array(windows).T
    df.sort_values(by="Windows", inplace=True)
    return df


def input_throughput_frame(js: dict):
    timestamps = []
    throughput = []

    for t in js.keys():
        timestamps.append(t)
        throughput.append(js[t])

    df = pd.DataFrame()
    windows = [datetime.utcfromtimestamp(int(timestamp) / 1000).strftime("%H:%M:%S") for timestamp in timestamps]
    df["Timestamps"] = np.array(windows).T
    df["Throughput"] = np.array(throughput).T
    df.sort_values(by="Timestamps", inplace=True)

    return df


def get_machine_ids(name_prefix):
    global machines
    with open(name_prefix + "machine_ids.txt", "r") as fh:
        lines = fh.readlines()
        lines = [line.rstrip() for line in lines]
        machines = lines
    print(machines)


if __name__ == '__main__':
    args = parseDrawArguments()
    experiment_name = args.name
    if not os.path.isdir(os.path.join(args.location, "experiments", experiment_name, "pngs")):
        os.makedirs(os.path.join(args.location, "experiments", experiment_name, "pngs"))
    output_dir = os.path.join(args.location, "experiments", experiment_name)
    name_prefix = output_dir+"/stats_files/" + args.name + "__"
    png_prefix = output_dir+"/pngs/" + args.name + "__"
    get_machine_ids(name_prefix)
    with open(name_prefix + "latency-percentiles-per-machine.txt", "r") as fh:
        js = json.load(fh)
    percentiles_frame = percentiles_dataframe(js)
    new_df = percentiles_frame.loc[:, (percentiles_frame.columns.get_level_values(0).drop_duplicates(), ["", "99%"])]
    print(new_df)
    draw_percentiles_line_chart(percentiles_frame, "99%", png_prefix)
    for machine in machines:
        draw_all_percentiles_per_machine(percentiles_frame, machine, png_prefix)
    with open(name_prefix + "final-comps-per-machine.txt", "r") as fh:
        m_comp_js = json.load(fh)
    computations_frame = machine_computations_dataframe(m_comp_js)
    print(computations_frame)
    draw_computations_all_machines(computations_frame, png_prefix)
    with open(name_prefix + "throughput.txt", "r") as fh:
        throughput_js = json.load(fh)
    throughput_frame = input_throughput_frame(throughput_js)
    draw_input_throughput(throughput_frame, png_prefix)
    #
    # with open(name_prefix+"size-per-group", "r") as fh:
    #     sizes_js = json.load(fh)
    # sizes_frame = group_size_dataframe(sizes_js)
    # print(sizes_frame)
