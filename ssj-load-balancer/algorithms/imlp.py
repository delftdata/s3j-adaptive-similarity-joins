import copy
import time
from collections import defaultdict
from functools import cmp_to_key, partial

import numpy as np

from src.utils import node_cost, cost_function, sorting_func


def sort_by_dist2optimal(item, opt):
    return abs(item[2] - opt)


def legal_move(group, mg_thd):
    mg_cost = group[-1]
    if mg_cost > mg_thd:
        return False
    else:
        return True


def legal_swap(group1, group2, mg_thd):
    mg_cost1 = group1[-1]
    mg_cost2 = group2[-1]
    if mg_cost1 + mg_cost2 > mg_thd:
        return False
    else:
        return True


def move1(bound, group):
    if bound > group[1] > 0:
        return True
    else:
        return False


def swap1(bound, max_group, other_group):
    if max_group[1] > other_group[1] and max_group[1] - other_group[1] < bound:
        return True
    else:
        return False


def candidate_moves_with_pen(optimal, bound, id_max, max_loaded, id_other, other, mg_thd, pen_matrix, g2i, n2i,
                             p_factor):
    candidate_groups = defaultdict(list, {k: [] for k in ("max", "other")})
    candidate_move1 = []
    candidate_swap1 = []
    for group in max_loaded:
        if legal_move(group, mg_thd):
            candidate_groups["max"].append(group)
            if move1(bound, group):
                if group[1] > optimal:
                    w = group[1] + (p_factor * pen_matrix[n2i[id_other]][g2i[str(group)]])
                else:
                    w = group[1] - (p_factor * pen_matrix[n2i[id_other]][g2i[str(group)]])
                candidate_move1.append(["1-move", group, w])

    for group in other:
        if legal_move(group, mg_thd):
            candidate_groups["other"].append(group)

    for max_group in candidate_groups["max"]:
        for other_group in candidate_groups["other"]:
            if (swap1(bound, max_group, other_group)) and (legal_swap(max_group, other_group, mg_thd)):
                if max_group[1] - other_group[1] > optimal:
                    w = max_group[1] - other_group[1] + (p_factor * (
                                pen_matrix[n2i[id_other]][g2i[str(max_group)]] +
                                pen_matrix[n2i[id_max]][g2i[str(other_group)]]))
                else:
                    w = max_group[1] - other_group[1] - (p_factor * (
                                pen_matrix[n2i[id_other]][g2i[str(max_group)]] +
                                pen_matrix[n2i[id_max]][g2i[str(other_group)]]))
                candidate_swap1.append(["1-swap", (max_group, other_group), w])

    candidates = candidate_move1 + candidate_swap1
    candidates.sort(reverse=False, key=partial(sort_by_dist2optimal, opt=optimal))
    if candidates:
        return candidates[0]
    else:
        return None


def multi_level_Local_Search_with_pen(initial_nodes, initial_group_assignment, target_cost, average,
                                      mg_levels, pen_matrix, group_to_index, node_to_index, p_factor):
    nodes_with_load = copy.deepcopy(initial_nodes)
    num_of_nodes = len(nodes_with_load)
    num_lvl = len(mg_levels)
    total_mg_cost = 0
    final_group_assignment = copy.deepcopy(initial_group_assignment)

    try:
        start_time = time.time()
        while 1:
            if time.time() - start_time > 1800:
                raise TimeoutError("Individual run exceeded time limit.")
            nodes_with_load.sort(reverse=True, key=sorting_func)
            max_loaded = nodes_with_load[0]
            updated = 0
            for i in range(num_lvl):
                for j in range(num_of_nodes - 1, 0, -1):
                    next_node = nodes_with_load[j]
                    optimal = (max_loaded[1] - next_node[1]) / 2
                    bound = max_loaded[1] - next_node[1]
                    selected_move = candidate_moves_with_pen(optimal, bound, max_loaded[0],
                                                             final_group_assignment[max_loaded[0]], next_node[0],
                                                             final_group_assignment[next_node[0]], mg_levels[i],
                                                             pen_matrix, group_to_index, node_to_index, p_factor)
                    if selected_move:
                        if selected_move[0] == "1-move":
                            moved_group = selected_move[1]
                            final_group_assignment[max_loaded[0]].remove(moved_group)
                            final_group_assignment[next_node[0]].append(moved_group)
                            upd_max_loaded = [max_loaded[0], max_loaded[1] - selected_move[1][1]]
                            upd_next_node = [next_node[0], next_node[1] + selected_move[1][1]]
                            nodes_with_load.remove(max_loaded)
                            nodes_with_load.append(upd_max_loaded)
                            nodes_with_load.remove(next_node)
                            nodes_with_load.append(upd_next_node)
                            updated = 1
                        elif selected_move[0] == "1-swap":
                            swap_group_max, swap_group_next = selected_move[1]
                            final_group_assignment[max_loaded[0]].remove(swap_group_max)
                            final_group_assignment[max_loaded[0]].append(swap_group_next)
                            final_group_assignment[next_node[0]].remove(swap_group_next)
                            final_group_assignment[next_node[0]].append(swap_group_max)
                            upd_max_loaded = [max_loaded[0],
                                              max_loaded[1] - selected_move[1][0][1] + selected_move[1][1][1]]
                            upd_next_node = [next_node[0],
                                             next_node[1] + selected_move[1][0][1] - selected_move[1][1][1]]
                            nodes_with_load.remove(max_loaded)
                            nodes_with_load.append(upd_max_loaded)
                            nodes_with_load.remove(next_node)
                            nodes_with_load.append(upd_next_node)
                            updated = 1
                        break
                if updated == 1:
                    break
            if updated == 0:
                break
            iteration_cost = cost_function(final_group_assignment, average)
            if iteration_cost < target_cost:
                fa = {}
                ia = {}
                for node in final_group_assignment.keys():
                    fa[node] = set([','.join([str(elem) for elem in s]) for s in final_group_assignment[node]])
                    ia[node] = set([','.join([str(elem) for elem in s]) for s in initial_group_assignment[node]])

                for key in final_group_assignment.keys():
                    for group in fa[key]:
                        if group not in ia[key]:
                            total_mg_cost += int(group.split(",")[-1])
                return final_group_assignment, iteration_cost, total_mg_cost

        iteration_cost = cost_function(final_group_assignment, average)
        fa = {}
        ia = {}
        for node in final_group_assignment.keys():
            fa[node] = set([','.join([str(elem) for elem in s]) for s in final_group_assignment[node]])
            ia[node] = set([','.join([str(elem) for elem in s]) for s in initial_group_assignment[node]])
        for key in final_group_assignment.keys():
            for group in fa[key]:
                if group not in ia[key]:
                    total_mg_cost += int(group.split(",")[-1])

        return final_group_assignment, iteration_cost, total_mg_cost

    except TimeoutError:
        return initial_group_assignment, None, None


def penalization(pen_matrix, group_to_index, node_to_index, initial_assignment, final_assignment):
    fa = {}
    ia = {}
    for node in final_assignment.keys():
        fa[node] = set([','.join([str(elem) for elem in s]) for s in final_assignment[node]])
        ia[node] = set([','.join([str(elem) for elem in s]) for s in initial_assignment[node]])

    for node in final_assignment.keys():
        for group in fa[node]:
            if group not in ia[node]:
                n_idx = node_to_index[node]
                group_list = group.split(",")
                group_key = str([group_list[0], int(group_list[1]), int(group_list[2])])
                g_idx = group_to_index[group_key]
                pen_matrix[n_idx][g_idx] += 1


class Run:
    def __init__(self, assignment, mg_cost, comp_cost):
        self.assignment = assignment
        self.mg_cost = mg_cost
        self.comp_cost = comp_cost

    def to_str(self):
        tmp = "\nComputation cost: " + str(self.comp_cost) + "\n\nMigration Cost: " + str(self.mg_cost)
        return tmp


def sort_runs_by_comp(item1, item2):
    if item1.comp_cost < item2.comp_cost:
        return -1
    elif item1.comp_cost == item2.comp_cost:
        if item1.mg_cost < item2.mg_cost:
            return -1
        else:
            return 1
    else:
        return 1


def sort_runs_by_mig(item1, item2):
    if item1.mg_cost < item2.mg_cost:
        return -1
    elif item1.mg_cost == item2.mg_cost:
        if item1.comp_cost < item2.comp_cost:
            return -1
        else:
            return 1
    else:
        return 1


def IMLP(nodes_with_costs, initial_assignment, initial_cost, target_cost, average, mg_levels, num_iter, p_factor):
    list_of_runs = []
    group_to_index = {}
    node_to_index = {}
    nodes = len(nodes_with_costs)
    groups = 0
    n_idx = 0
    g_idx = 0

    for k in initial_assignment.keys():
        node_to_index[k] = n_idx
        n_idx += 1
        groups += len(initial_assignment[k])
        for g in initial_assignment[k]:
            group_to_index[str(g)] = g_idx
            g_idx += 1

    pen_matrix = np.zeros((nodes, groups), dtype=int)

    if target_cost > initial_cost:
        return initial_assignment
    try:
        start_time = time.time()
        for i in range(num_iter):
            if time.time() - start_time > 1800:
                raise TimeoutError("Algorithm execution exceeded time limit!")
            assignment, comp_cost, mg_cost = multi_level_Local_Search_with_pen(copy.deepcopy(nodes_with_costs),
                                                                               initial_assignment, target_cost, average, mg_levels,
                                                                               pen_matrix, group_to_index,
                                                                               node_to_index, p_factor)
            list_of_runs.append(Run(assignment, mg_cost, comp_cost))
            if time.time() - start_time > 1800:
                raise TimeoutError("Algorithm execution exceeded time limit!")
            penalization(pen_matrix, group_to_index, node_to_index, initial_assignment, assignment)
    except TimeoutError:
        if list_of_runs:
            list_of_runs.sort(key=cmp_to_key(sort_runs_by_comp))
            best_run = list_of_runs[0]
            if best_run.comp_cost is None:
                list_of_runs.remove(best_run)
                if list_of_runs:
                    best_run = list_of_runs[0]
                else:
                    return None, None
            list_of_runs.sort(key=cmp_to_key(sort_runs_by_mig))
            best_mig_run = list_of_runs[0]
            if best_mig_run.comp_cost is None:
                list_of_runs.remove(best_mig_run)
                if list_of_runs:
                    best_mig_run = list_of_runs[0]
                else:
                    return None, None
            return best_run, best_mig_run

        else:
            return None, None

    list_of_runs.sort(key=cmp_to_key(sort_runs_by_comp))
    best_run = list_of_runs[0]
    list_of_runs.sort(key=cmp_to_key(sort_runs_by_mig))
    best_mig_run = list_of_runs[0]

    return best_run, best_mig_run


def IMLP_wrapper(unbalanced_groups, average, max_mg_cost, runs, p_factor):
    nodes_with_costs = []
    for node in unbalanced_groups.keys():
        nodes_with_costs.append([node, node_cost(unbalanced_groups[node])])

    initial_cost = cost_function(unbalanced_groups, average)

    best_run, best_mig_run = IMLP(nodes_with_costs, unbalanced_groups, initial_cost, initial_cost * 0.1, average,
                                  [x * 2 * max_mg_cost for x in [0.10, 0.30, 0.70, 1]], runs, p_factor)

    if best_run is None or best_mig_run is None:
        print("Algorithm execution exceeded time limit!")
        return None, None, None, None
    else:
        return best_run.comp_cost, best_run.mg_cost, best_run.assignment, best_mig_run.comp_cost, best_mig_run.mg_cost, \
               best_mig_run.assignment
