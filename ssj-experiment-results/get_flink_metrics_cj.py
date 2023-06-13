import itertools
import json
import os
import sys
import time
import pandas as pd
import numpy as np
import requests

import matplotlib.pyplot as plt

import config

if __name__ == "__main__":
    args = config.parseFlinkMetrics()
    metrics = args.operator_metrics
    experiment_name = args.experiment_name
    all_zero = False
    all_zero_again = False
    started = False
    to_continue = False

    metrics_dict = {}

    cj_jobid_request = requests.get(f'http://coordinator:30080/get_cj_jobid')
    cj_id = cj_jobid_request.text

    if not os.path.isdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), "experiments", experiment_name)):
        os.makedirs(os.path.join(os.path.dirname(os.path.abspath(__file__)), "experiments", experiment_name))

    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "experiments", experiment_name)

    r = requests.get(f'http://flink-rest:30080/jobs/{cj_id}/plan')
    vertix_id = r.json()["plan"]["nodes"][0]["id"]
    parallelism = r.json()["plan"]["nodes"][0]["parallelism"]

    for s in range(parallelism):
        metrics_dict[s] = {}
        for m in metrics.split(","):
            metrics_dict[s][m] = []
    while True:
        to_continue = False
        metrics_returned = {}
        for subtask_id in range(parallelism):
            b = requests.get(f'http://flink-rest:30080/jobs/{cj_id}/vertices/{vertix_id}/subtasks/{subtask_id}/metrics'
                             f'?get={metrics}')
            # print(subtask_id, b.json())
            if not b.json():
                to_continue = True
                break
            metrics_returned[subtask_id] = b.json()
        if to_continue:
            continue
        for subtask_id in range(parallelism):
            for idx in range(len(metrics_returned[subtask_id])):
                if float(metrics_returned[subtask_id][idx]["value"]) > 0.5:
                    if not started:
                        started = True
                        print("SSJ job started...")
                metrics_dict[subtask_id][metrics_returned[subtask_id][idx]["id"]]\
                    .append(float(metrics_returned[subtask_id][idx]["value"]))
        if started:
            all_zero = True
            for subtask_id in range(parallelism):
                for metric in metrics_dict[subtask_id].keys():
                    if metrics_dict[subtask_id][metric][-1] > 0.5:
                        all_zero = False
                        break

        if all_zero and all_zero_again and started:
            break
        elif all_zero and not all_zero_again:
            all_zero_again = True
            all_zero = False
        elif not all_zero and all_zero_again:
            all_zero = False
            all_zero_again = False
        time.sleep(10)

    with open(results_dir+"/metrics_dict.json", "w") as fp:
        fp.write(json.dumps(metrics_dict, indent=4))

    metric_values = []
    for s in range(parallelism):
        for m in metrics.split(","):
            metric_values.append(metrics_dict[s][m])

    # print(metric_values)
    np_metric_values = np.array(metric_values)
    df = pd.DataFrame(data=np_metric_values.T,
                      columns=pd.MultiIndex.from_tuples(
                          itertools.product([s for s in range(parallelism)],metrics.split(",")))
                      )
    for m in metrics.split(","):
        new_df = df.loc[:, (df.columns.get_level_values(0).drop_duplicates(), [m])]
        new_df.columns = new_df.columns.rename("Machine", level=0)
        new_df.columns = new_df.columns.rename("Metric", level=1)
        pl = new_df.plot(y=new_df.columns, use_index=True,
                         ylabel="Rate(r/s)", marker="o")
        plt.legend(bbox_to_anchor=(0.8, -0.1))
        plt.savefig(results_dir + "/" + m + ".png", bbox_inches="tight")
        plt.plot()
