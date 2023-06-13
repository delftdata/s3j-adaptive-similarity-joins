import itertools

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import json

import config

if __name__ == "__main__":
    experiment_name = "1min-120mon-10rate-95sim-uniform"
    results_dir = os.path.join(os.path.join(os.getcwd(), "experiments", experiment_name))
    parallelism = 3
    metrics_dict = {}

    with open(results_dir+"/metrics_dict.json", "r") as fp:
        metrics_dict = json.load(fp)

    metric_values = []
    for s in range(parallelism):
        for m in metrics_dict[str(s)].keys():
            metric_values.append(metrics_dict[str(s)][m])

    print(metric_values)
    for l in range(len(metric_values)):
        print(len(metric_values[l]))
    np_metric_values = np.array(metric_values, dtype=object)
    df = pd.DataFrame(data=np_metric_values.T,
                      columns=pd.MultiIndex.from_tuples(
                          itertools.product([str(s) for s in range(parallelism)], metrics_dict["0"].keys()))
                      )
    for m in metrics_dict["0"].keys():
        new_df = df.loc[:, (df.columns.get_level_values(0).drop_duplicates(), [m])]
        new_df.columns = new_df.columns.rename("Machine", level=0)
        new_df.columns = new_df.columns.rename("Metric", level=1)
        pl = new_df.plot(y=new_df.columns, use_index=True,
                         ylabel="Rate(r/s)")
        plt.legend(bbox_to_anchor=(0.8, -0.1))
        plt.savefig(results_dir + "/" + m + ".png", bbox_inches="tight")
        plt.close()
