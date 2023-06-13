import itertools
import json
import os
import random
from datetime import datetime

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import matplotlib


from config import parseDrawArguments

percent_keys = ["50%", "90%", "95%", "99%", "99.9%"]
machines = []

filenames = [
    "final-comps-per-machine",
    "final-comps-per-group",
    "size-per-group",
    "latency-percentiles-per-machine"]

linestyle_str = [
     ('solid', 'solid'),      # Same as (0, ()) or '-'
     ('dotted', 'dotted'),    # Same as (0, (1, 1)) or ':'
     ('dashed', 'dashed'),    # Same as '--'
     ('dashdot', 'dashdot')] 


linestyle_tuple = [
     ('solid', (0, ())),
     ('loosely dotted',        (0, (1, 10))),
     ('dotted',                (0, (1, 1))),
     ('densely dotted',        (0, (1, 1))),

     ('loosely dashed',        (0, (5, 10))),
     ('dashed',                (0, (5, 5))),
     ('densely dashed',        (0, (5, 1))),

     ('loosely dashdotted',    (0, (3, 10, 1, 10))),
     ('dashdotted',            (0, (3, 5, 1, 5))),
     ('densely dashdotted',    (0, (3, 1, 1, 1))),

     ('dashdotdotted',         (0, (3, 5, 1, 5, 1, 5))),
     ('loosely dashdotdotted', (0, (3, 10, 1, 10, 1, 10))),
     ('densely dashdotdotted', (0, (3, 1, 1, 1, 1, 1)))]

avail_markers = ["o", "X", "s", "*", "D"]

matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

def draw_computations_all_machines(df: pd.DataFrame, filename: str):
    pl = df.plot("Windows", df.columns[:-1], ylabel="Number of computations", marker="o",
                 title="Final Computations")
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
                     ylabel="Latency(s)", marker="o")
    plt.legend(bbox_to_anchor=(1.01, 1.0))
    plt.savefig(filename+'machine_%s-all_percentiles.png' % machine, bbox_inches="tight")
    plt.close()


def draw_percentiles_line_chart(df: pd.DataFrame, perc: str, filename: str):
    new_df = df.loc[:, (df.columns.get_level_values(0).drop_duplicates(), ["", perc])]
    new_df.loc[:, (df.columns.get_level_values(0).drop_duplicates()[:-1], perc)] =\
        new_df.loc[:, (df.columns.get_level_values(0).drop_duplicates()[:-1], perc)].div(1000)
    new_df.columns = new_df.columns.rename('MachineID', level=0)
    new_df.columns = new_df.columns.rename('Percentile', level=1)
    print(new_df.columns)
    pl = new_df.plot("Windows", new_df.columns.get_level_values(0).drop_duplicates()[:-1],
                     ylabel="Latency(s)", figsize=(8, 2.3))

    avail_markers = ["o", "X", "s", "*", "D"]
    for i, line in enumerate(pl.get_lines()):
        line.set_marker(avail_markers[i])
    # for i, p in enumerate(new_df["0"][perc]):
    #     plt.text(i, p+20, "%.2f" %p, ha="center", color="magenta")
    new_df_title = "(MachineID, Percentile)"
    # plt.legend(title=new_df_title, loc='upper center',
    #              bbox_to_anchor=(0.5, -0.1), ncol=5)
    plt.savefig(filename+'%s-percentile.png' % perc, bbox_inches="tight")
    plt.close()


def draw_single(df: pd.DataFrame, perc: str, legend: bool, xticks: list, yticks=None ,ax=None):

    styles = []
    for i, (name, linestyle) in enumerate(linestyle_str):
        styles.append(linestyle)
    print(styles)

    new_df = df.loc[:, (df.columns.get_level_values(0).drop_duplicates(), ["", perc])]
    new_df.loc[:, (df.columns.get_level_values(0).drop_duplicates()[:-1], perc)] = \
        new_df.loc[:, (df.columns.get_level_values(0).drop_duplicates()[:-1], perc)].div(1000)
    new_df.columns = new_df.columns.rename('WorkerID', level=0)
    new_df.columns = new_df.columns.rename('Percentile', level=1)

    for worker in new_df.columns.get_level_values(0).drop_duplicates()[:-1]:
        print(worker)

    if yticks is not None:
        for i, worker in enumerate(new_df.columns.get_level_values(0).drop_duplicates()[:-1]):
            mar = avail_markers[i%len(avail_markers)]
            if mar == "X" or mar == "*":
                markersize=4
            else:
                markersize=3
            pl = new_df.plot.line("Time(s)", worker,
                     ylabel="Latency(s)", ax=ax, legend=False, xticks=xticks, yticks=yticks, linestyle=styles[i%len(styles)], 
                     marker=mar, markevery=(0.1,0.1), markersize=markersize, linewidth=1)
    else:
        for i, worker in enumerate(new_df.columns.get_level_values(0).drop_duplicates()[:-1]):
            mar = avail_markers[i%len(avail_markers)]
            if mar == "X" or mar == "*":
                markersize=4
            else:
                markersize=3
            pl = new_df.plot.line("Time(s)", worker,
                     ylabel="Latency(s)", ax=ax, legend=False, xticks=xticks, linestyle=styles[i%len(styles)],
                     marker=mar, markevery=(0.1,0.1), markersize=markersize, linewidth=1)

    # avail_markers = ["o", "X", "s", "*", "D", "o", "X", "s", "*", "D", "o", "X", "s", "*", "D", "o", "X", "s", "*", "D"]
    # for i, line in enumerate(pl.get_lines()):
    #     line.set_marker(avail_markers[i])
    # for i, p in enumerate(new_df["0"][perc]):
    #     plt.text(i, p+20, "%.2f" %p, ha="center", color="magenta")
    new_df_title = "(WorkerID, Percentile)"
    if legend:
        plt.legend(title=new_df_title, loc='upper center', bbox_to_anchor=(0.5,-0.3), fancybox=False, shadow=False, ncol=5)
    return pl


def draw_percentiles_line_chart_3in1(df1: pd.DataFrame, df2: pd.DataFrame, df3: pd.DataFrame, perc: str, filename: str):
    # new_df = df1.loc[:, (df1.columns.get_level_values(0).drop_duplicates(), ["", perc])]
    fig, axes = plt.subplots(nrows=1, ncols=3)
    fig.set_size_inches(14, 2)
    plt.subplots_adjust(hspace=0.6)
    pl = draw_single(df1, perc, False, axes[0])
    pl = draw_single(df2, perc, False, axes[1])
    pl = draw_single(df3, perc, False, axes[2])
    axes[1].legend(title="(WorkerID, Percentile)",loc='upper center',
                 bbox_to_anchor=(0.5, -0.3), fancybox=False, shadow=False, ncol=8)
    axes[0].title.set_text("selectivity = 1%")
    axes[1].title.set_text("selectivity = 0.5%")
    axes[2].title.set_text("selectivity = 0.1%")

    fig.savefig(filename+'%s-percentile.png' % perc, bbox_inches="tight")
    plt.close()


def draw_single_line_chart(df: pd.DataFrame, perc: str, filename: str, type: str):
    fig = plt.figure()
    fig, axes = plt.subplots(nrows=1, ncols=1)
    fig.set_size_inches(7, 3)
    pl = draw_single(df, perc, False, [0, 60, 120, 180, 240], ax=axes)
    ttl= axes.title
    ttl.set_text("$\mathregular{S^3J}$")
    fig.savefig(filename+'%s-percentile.pdf' % perc.replace('%',""), bbox_inches="tight")
    plt.close()

def draw_percentiles_line_chart_6in1(df1: pd.DataFrame, df2: pd.DataFrame, df3: pd.DataFrame, df4: pd.DataFrame, df5: pd.DataFrame, df6: pd.DataFrame, perc: str, filename: str, type: str):
    # new_df = df1.loc[:, (df1.columns.get_level_values(0).drop_duplicates(), ["", perc])]
    fig = plt.figure()
    fig, axes = plt.subplots(nrows=2, ncols=3)
    fig.set_size_inches(14, 5)
    plt.subplots_adjust(hspace=0.5)
    if(type=="selectivity"):
        pl = draw_single(df1, perc, False, [0,100,200,300,400,500, 600], ax=axes[0][0])
        pl = draw_single(df2, perc, False, [0,50,150,250,350,450], ax=axes[0][1])
        pl = draw_single(df3, perc, False, [0,100,200,300], yticks=[0, 0.5, 1.0], ax=axes[0][2])
        pl = draw_single(df4, perc, False, [0,100,200,300,400,500, 600], ax=axes[1][0])
        pl = draw_single(df5, perc, False, [0,50,150,250,350,450], ax=axes[1][1])
        pl = draw_single(df6, perc, False, [0,100,200,300], ax=axes[1][2])
    else:
        pl = draw_single(df1, perc, False, [0,50,150,250,350,450,550], ax=axes[0][0])
        pl = draw_single(df2, perc, False, [0,50,150,250,350], ax=axes[0][1])
        pl = draw_single(df3, perc, False, [0,100,200,300,400], yticks=[0, 0.5, 1.0], ax=axes[0][2])
        pl = draw_single(df4, perc, False, [0,50,150,250,350,450,550], ax=axes[1][0])
        pl = draw_single(df5, perc, False, [0,50,150,250,350], ax=axes[1][1])
        pl = draw_single(df6, perc, False, [0,100,200,300,400], ax=axes[1][2])
    # axes[1][1].legend(title="(WorkerID, Percentile)",loc='upper center',
    #              bbox_to_anchor=(0.5, -0.3), fancybox=False, shadow=False, ncol=8)
    axes[1][1].title.set_text("ClusterJoin")
    if(type=="selectivity"):
        axes[0][0].title.set_text("selectivity = 1%")
        axes[0][1].title.set_text("$\mathregular{S^3J}$\nselectivity = 0.5%")
        axes[0][2].title.set_text("selectivity = 0.1%")
    if(type=="parallelism"):
        axes[0][0].title.set_text("parallelism = 5")
        axes[0][1].title.set_text("$\mathregular{S^3J}$\nparallelism = 10")
        axes[0][2].title.set_text("parallelism = 15")

    fig.savefig(filename+'%s-percentile.pdf' % perc.replace('%',""), bbox_inches="tight")
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
    # windows = [datetime.utcfromtimestamp(int(timestamp) / 1000).strftime("%H:%M:%S") for timestamp in timestamps]
    windows = [idx*5 for idx, _ in enumerate(timestamps)]
    df["Time(s)"] = np.array(windows).T
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
    windows = [idx for idx, _ in enumerate(timestamps)]
    df["Windows"] = np.array(windows).T
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
    windows = [idx for idx, _ in enumerate(timestamps)]
    df["Windows"] = np.array(windows).T
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
    experiment_names = args.name.split(",")
    print(len(experiment_names))
    # if len(experiment_names) == 1:
    #     experiment_name = experiment_names[0]
    #     input_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "experiments", experiment_name)
    #     if not os.path.isdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_figures", experiment_name)):
    #         os.makedirs(os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_figures", experiment_name))
    #     output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_figures", experiment_name)
    #     name_prefix = input_dir+"/stats_files/" + args.name + "__"
    #     png_prefix = output_dir +"/"+ args.name + "__"
    #     get_machine_ids(name_prefix)
    #     with open(name_prefix + "latency-percentiles-per-machine.txt", "r") as fh:
    #         js = json.load(fh)
    #     percentiles_frame = percentiles_dataframe(js)
    #     new_df = percentiles_frame.loc[:, (percentiles_frame.columns.get_level_values(0).drop_duplicates(), ["", "99%"])]
    #     # print(new_df)
    #     draw_percentiles_line_chart(percentiles_frame, "99%", png_prefix)
    #     for machine in machines:
    #         draw_all_percentiles_per_machine(percentiles_frame, machine, png_prefix)
    #     with open(name_prefix + "final-comps-per-machine.txt", "r") as fh:
    #         m_comp_js = json.load(fh)
    #     computations_frame = machine_computations_dataframe(m_comp_js)
    #     # print(computations_frame)
    #     draw_computations_all_machines(computations_frame, png_prefix)

    if args.paper and not args.type:
        input_dir_arr = []
        for exp in experiment_names:
            input_dir_arr.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "experiments", exp))
        if not os.path.isdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_figures", "combined_"+args.type)):
            os.makedirs(os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_figures", "combined_"+args.type))
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_figures", "combined_"+args.type)
        dataframes = []
        for idx, exp in enumerate(experiment_names):
            name_prefix = input_dir_arr[idx]+"/stats_files/" + exp + "__"
            png_prefix = output_dir +"/"+ args.type + "__"
            get_machine_ids(name_prefix)
            with open(name_prefix + "latency-percentiles-per-machine.txt", "r") as fh:
                js = json.load(fh)
            percentiles_frame = percentiles_dataframe(js)
            dataframes.append(percentiles_frame)
        draw_percentiles_line_chart_3in1(dataframes[0], dataframes[1], dataframes[2], "99%", png_prefix)

    elif args.paper and (args.type=="selectivity" or args.type=="parallelism"):
        input_dir_arr = []
        for exp in experiment_names:
            input_dir_arr.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "experiments", exp))
        if not os.path.isdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_figures", "combined_"+args.type)):
            os.makedirs(os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_figures", "combined_"+args.type))
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_figures", "combined_"+args.type)
        dataframes = []
        for idx, exp in enumerate(experiment_names):
            name_prefix = input_dir_arr[idx]+"/stats_files/" + exp + "__"
            png_prefix = output_dir +"/"+ args.type + "__"
            get_machine_ids(name_prefix)
            with open(name_prefix + "latency-percentiles-per-machine.txt", "r") as fh:
                js = json.load(fh)
            percentiles_frame = percentiles_dataframe(js)
            dataframes.append(percentiles_frame)
        draw_percentiles_line_chart_6in1(dataframes[0], dataframes[1], dataframes[2], dataframes[3], dataframes[4], dataframes[5], "99%", png_prefix, args.type)

    elif args.paper and args.type=="balancing":
        input_dir_arr = []
        for exp in experiment_names:
            input_dir_arr.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "experiments", exp))
        if not os.path.isdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_figures", args.type)):
            os.makedirs(os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_figures", args.type))
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_figures", args.type)
        dataframes = []
        for idx, exp in enumerate(experiment_names):
            name_prefix = input_dir_arr[idx]+"/stats_files/" + exp + "__"
            png_prefix = output_dir +"/"+ args.type + "__"
            get_machine_ids(name_prefix)
            with open(name_prefix + "latency-percentiles-per-machine.txt", "r") as fh:
                js = json.load(fh)
            percentiles_frame = percentiles_dataframe(js)
            dataframes.append(percentiles_frame)
        draw_single_line_chart(dataframes[0], "99%", png_prefix, args.type)
        

    else:
        input_dir_arr = []
        for exp in experiment_names:
            input_dir_arr.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "experiments", exp))
        if not os.path.isdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_figures", "combined_"+args.type)):
            os.makedirs(os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_figures", "combined_"+args.type))
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_figures", "combined_"+args.type)
        dataframes = []
        for idx, exp in enumerate(experiment_names):
            name_prefix = input_dir_arr[idx]+"/stats_files/" + exp + "__"
            png_prefix = output_dir +"/"+ args.type + "__"
            get_machine_ids(name_prefix)
            with open(name_prefix + "latency-percentiles-per-machine.txt", "r") as fh:
                js = json.load(fh)
            percentiles_frame = percentiles_dataframe(js)
            dataframes.append(percentiles_frame)
        draw_percentiles_line_chart_3in1(dataframes[0], dataframes[1], dataframes[1], "99%", png_prefix)
