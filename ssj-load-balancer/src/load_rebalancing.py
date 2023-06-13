import copy
from functools import partial

from wrappers import *


def format_lb_input(group_comps: dict, group_sizes: dict, machine_comps: dict):
    assignment = dict.fromkeys(group_sizes.keys())
    for m in assignment.keys():
        assignment[m] = []
        for k in group_sizes[m].keys():
            if k in group_comps[m].keys():
                assignment[m].append((k, group_comps[m][k], group_sizes[m][k]))
            else:
                assignment[m].append((k, 0, group_sizes[m][k]))

    load_sum = 0
    count = len(machine_comps.keys())
    for m in machine_comps.keys():
        if machine_comps[m] is None:
            load_sum += 0
        else:
            load_sum += machine_comps[m]

    return assignment, load_sum/count


class Result:
    def __init__(self, load_imb, migration, assignment, runtime):
        self.load_imbalance = load_imb
        self.migration = migration
        self.assignment = assignment
        self.runtime = runtime

class Rebalancer:

    def __init__(self, algos, metric, config):
        self.algorithms = algos
        self.minimization_metric = metric
        self.config = config
        self.wrappers = {
            "aaa": aaa_wrapper,
            "aaa_m": aaa_m_wrapper,
            "mr-partition": partial(mr_partition_wrapper,
                                    runs=int(config["Algorithms"]["mr_partition.runs"])),
            "greedy": greedy_wrapper,
            "imlp_m": partial(imlp_wrapper,
                              max_mig_cost=int(config["Algorithms"]["imlp.max_mig_cost"]),
                              runs=int(config["Algorithms"]["imlp.runs"]),
                              p_factor=int(config["Algorithms"]["imlp.p_factor"]),
                              mode='m'
                              ),
            "imlp_c": partial(imlp_wrapper,
                              max_mig_cost=int(config["Algorithms"]["imlp.max_mig_cost"]),
                              runs=int(config["Algorithms"]["imlp.runs"]),
                              p_factor=int(config["Algorithms"]["imlp.p_factor"]),
                              mode='c'
                              )
        }

    def create_new_assignment(self, old_assignment, average):
        results = dict.fromkeys(self.algorithms)
        for algo in self.algorithms:
            wa = self.wrappers.get(algo)
            input_assignment = copy.deepcopy(old_assignment)
            overall, migration_cost, new_assignment, runtime = wa(input_assignment, average)
            results[algo] = Result(overall, migration_cost, new_assignment, runtime)

        best_result = None
        best_algo = None
        for algo in results.keys():
            if best_algo is None:
                best_result = results[algo]
                best_algo = algo
            else:
                if self.minimization_metric == "load_imbalance":
                    if best_result.load_imbalance > results[algo].load_imbalance:
                        best_result = results[algo]
                        best_algo = algo
                elif self.minimization_metric == "migration":
                    if best_result.migration > results[algo].migration:
                        best_result = results[algo]
                        best_algo = algo
                else:
                    if best_result.runtime > results[algo].runtime:
                        best_result = results[algo]
                        best_algo = algo

        return best_algo, best_result
