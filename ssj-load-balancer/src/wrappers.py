import time

from algorithms.aaa import aaa
from algorithms.aaa_m import aaa_m
from algorithms.greedy_algorithm import greedy_2app
from algorithms.imlp import IMLP_wrapper
from algorithms.partition_algorithm import partition_multiple_runs
from utils import over_under_classification, node_cost


def aaa_wrapper(old_assignment, average):
    over, under = over_under_classification(old_assignment, average)
    start_time = time.time()
    load_imbalance, migration, assignment = aaa(over, under, average)
    runtime = time.time() - start_time
    return load_imbalance, migration, assignment, runtime


def aaa_m_wrapper(old_assignment, average):
    over, under = over_under_classification(old_assignment, average)
    start_time = time.time()
    load_imbalance, migration, assignment = aaa_m(over, under, average)
    runtime = time.time() - start_time
    return load_imbalance, migration, assignment, runtime


def mr_partition_wrapper(old_assignment, average, runs):
    nodes_with_loads = []
    for node in old_assignment.keys():
        nodes_with_loads.append([node, node_cost(old_assignment[node])])
    start_time = time.time()
    load_imbalance, migration, assignment = partition_multiple_runs(runs, nodes_with_loads, old_assignment, average)
    runtime = time.time() - start_time
    return load_imbalance, migration, assignment, runtime


def greedy_wrapper(old_assignment, average):
    nodes_with_loads = []
    for node in old_assignment.keys():
        nodes_with_loads.append([node, node_cost(old_assignment[node])])
    start_time = time.time()
    load_imbalance, migration, assignment = greedy_2app(nodes_with_loads, old_assignment, average)
    runtime = time.time() - start_time
    return load_imbalance, migration, assignment, runtime


def imlp_wrapper(old_assignment, average, max_mig_cost, runs, p_factor, mode):
    start_time = time.time()
    c_load_imbalance, c_migration, c_assignment, m_load_imbalance, m_migration, m_assignment = \
        IMLP_wrapper(old_assignment, average, max_mig_cost, runs, p_factor)
    runtime = time.time() - start_time
    if mode == "c":
        return c_load_imbalance, c_migration, c_assignment, runtime
    else:
        return m_load_imbalance, m_migration, m_assignment, runtime

