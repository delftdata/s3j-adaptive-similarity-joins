import os

import pandas as pd
import json
import matplotlib.pyplot as plt

from draw import percentiles_dataframe

machines = ["0", "13", "26", "39", "52", "64", "77", "90", "103", "116"]
percent_keys = ["50%", "90%", "95%", "99%", "99.9%"]


def draw_average_latency_percentile(df: pd.DataFrame, filename: str = None):
    df["mean"] = new_df.iloc[:, :-1].mean(axis=1)
    # print(new_df)
    df.iloc[:, -2:].plot("Windows", "mean")
    plt.show()
    plt.close()


def draw_max_latency_percentile(df: pd.DataFrame, filename: str = None):
    df["max"] = new_df.iloc[:, :-2].max(axis=1)
    print(df)
    df.plot("Windows", "max")
    plt.show()
    plt.close()


if __name__ == "__main__":
    experiment_name = "10min-1minWin-5mon-2000rate-995sim-10p-uniform2D"
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "experiments", experiment_name)
    name_prefix = output_dir+"/stats_files/" + experiment_name + "__"
    with open(name_prefix + "latency-percentiles-per-machine.txt", "r") as fh:
        js = json.load(fh)
    # print(js)
    percentiles_frame = percentiles_dataframe(js)
    new_df = percentiles_frame.loc[:, (percentiles_frame.columns.get_level_values(0).drop_duplicates(), ["", "99%"])]
    draw_average_latency_percentile(new_df)
    draw_max_latency_percentile(new_df)

