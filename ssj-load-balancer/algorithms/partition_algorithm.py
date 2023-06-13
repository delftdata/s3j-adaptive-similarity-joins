import copy, time
from collections import defaultdict
from functools import cmp_to_key

from src.utils import sorting_func, node_cost, cost_function


def sort_by_ci(a, b):
    if a[2] > b[2]:
        return 1
    elif a[2] == b[2]:
        if a[1] < b[1]:
            return 1
        else:
            return -1
    else:
        return -1


def remove_large_groups(unbalanced_groups, optimal):
    removed = []
    large_free_nodes = []
    large_groups = 0
    for node in unbalanced_groups.keys():
        least_large_group = None
        for group in unbalanced_groups[node]:
            if group[1] > 1 / 2 * optimal:
                large_groups += 1
                if least_large_group is None:
                    least_large_group = group
                elif least_large_group[1] > group[1]:
                    unbalanced_groups[node].remove(least_large_group)
                    removed.append(least_large_group)
                    least_large_group = group
                else:
                    removed.append(group)
                    unbalanced_groups[node].remove(group)
        if least_large_group is None:
            large_free_nodes.append(node)

    return removed, large_free_nodes, large_groups


def estimate_optimal(unbalanced_groups, average):
    optimal = None
    for node in unbalanced_groups.keys():
        for group in unbalanced_groups[node]:
            if optimal is None:
                optimal = group[1]
            elif optimal < group[1]:
                optimal = group[1]
            else:
                continue

            if optimal < average:
                optimal = average

    return optimal


def find_a(node_groups, node_cost, optimal):
    node_groups.sort(reverse=True, key=sorting_func)
    count = 0
    for group in node_groups:
        if node_cost < 1 / 2 * optimal:
            break
        if group[1] < 1 / 2 * optimal:
            count += 1
            node_cost -= group[1]

    return count


def find_b(node_groups, node_cost, optimal):
    node_groups.sort(reverse=True, key=sorting_func)
    count = 0
    for group in node_groups:
        if node_cost <= optimal:
            break
        else:
            node_cost -= group[1]
            count += 1

    return count


def find_c(a, b):
    return a - b


def remove_a_groups(node_groups, a, optimal):
    removed = []
    node_groups.sort(reverse=True, key=sorting_func)
    for group in node_groups:
        if a == 0:
            break
        if group[1] < 1 / 2 * optimal:
            node_groups.remove(group)
            removed.append(group)
            a -= 1

    return removed


def remove_b_groups(node_groups, b, optimal):
    removed = []
    large_groups = []
    node_groups.sort(reverse=True, key=sorting_func)
    for group in node_groups:
        if b == 0:
            break
        if group[1] > 1 / 2 * optimal:
            large_groups.append(group)
        else:
            removed.append(group)
        node_groups.remove(group)
        b -= 1

    return removed, large_groups


def reassign_small_groups(unbalanced_groups, nodes_with_costs, small_groups):
    for sm in small_groups:
        nodes_with_costs.sort(key=sorting_func)
        nodes_with_costs[0][1] += sm[1]
        unbalanced_groups[nodes_with_costs[0][0]].append(sm)


def partition(unbalanced_nodes, unbalanced_groups, average):

    optimal = estimate_optimal(unbalanced_groups, average)
    removed_large_groups, large_free_nodes, num_of_large_groups = remove_large_groups(unbalanced_groups, optimal)

    node_values = defaultdict(list)
    sorted_nodes = []

    num_of_nodes = len(unbalanced_groups.keys())

    for node in unbalanced_groups.keys():
        node_list = []
        nc = node_cost(unbalanced_groups[node])
        node_values[node].append(find_a(unbalanced_groups[node], nc, optimal))
        node_values[node].append(find_b(unbalanced_groups[node], nc, optimal))
        node_values[node].append(find_c(node_values[node][0], node_values[node][1]))
        node_list.append(node)
        if node in large_free_nodes:
            node_list.append(0)
        else:
            node_list.append(1)
        node_list.append(node_values[node][2])
        sorted_nodes.append(node_list)

    sorted_nodes.sort(key=cmp_to_key(sort_by_ci))

    indicator = 0
    large_free_step3 = []
    small_groups = []
    for node in sorted_nodes:
        if indicator < num_of_large_groups:
            a_groups = remove_a_groups(unbalanced_groups[node[0]], node_values[node[0]][0], optimal)
            indicator += 1
            if node[1] == 0:
                large_free_step3.append([node[0], node_cost(unbalanced_groups[node[0]])])
            small_groups += a_groups
        else:
            b_groups, large_groups = remove_b_groups(unbalanced_groups[node[0]], node_values[node[0]][1], optimal)
            for g in large_groups:
                large_free_step3.sort(key=sorting_func)
                unbalanced_groups[large_free_step3[0][0]].append(g)
                large_free_nodes.remove(large_free_step3[0][0])
                large_free_step3.remove(large_free_step3[0])
            small_groups += b_groups

    large_free_with_costs = []
    for node in large_free_nodes:
        large_free_with_costs.append([node, node_cost(unbalanced_groups[node])])

    for lg in removed_large_groups:
        large_free_with_costs.sort(key=sorting_func)
        unbalanced_groups[large_free_with_costs[0][0]].append(lg)

    nodes_with_costs = []
    for node in unbalanced_groups.keys():
        nodes_with_costs.append([node, node_cost(unbalanced_groups[node])])

    reassign_small_groups(unbalanced_groups, nodes_with_costs, small_groups)

    return unbalanced_nodes, unbalanced_groups, cost_function(unbalanced_groups, average)


class BestRun:
    unbalanced_groups = None
    unbalanced = None
    cost = None


def partition_multiple_runs(runs, unbalanced_nodes, unbalanced_groups, average):
    initial_groups = copy.deepcopy(unbalanced_groups)

    best_run = BestRun()
    start_time = time.time()

    for i in range(0, runs):
        if time.time() - start_time > 1800:
            print("AAA: Exceeded time limit.")
            break
        unbalanced, unbalanced_groups, cost = partition(unbalanced_nodes, unbalanced_groups, average)
        if best_run.cost is None:
            best_run.unbalanced_groups = copy.deepcopy(unbalanced_groups)
            best_run.unbalanced = copy.deepcopy(unbalanced)
            best_run.cost = cost
        elif best_run.cost > cost:
            best_run.unbalanced_groups = copy.deepcopy(unbalanced_groups)
            best_run.unbalanced = copy.deepcopy(unbalanced)
            best_run.cost = cost

    unbalanced_groups = best_run.unbalanced_groups
    #     print("\nFinal cost: " + str(best_run.cost)+"\n")
    #     print(unbalanced_groups)

    mig_cost = 0
    fa = {}
    ia = {}
    for node in unbalanced_groups.keys():
        fa[node] = set([','.join([str(elem) for elem in s]) for s in unbalanced_groups[node]])
        ia[node] = set([','.join([str(elem) for elem in s]) for s in initial_groups[node]])
    for key in unbalanced_groups.keys():
        for group in fa[key]:
            if group not in ia[key]:
                mig_cost += int(group.split(",")[-1])

    return best_run.cost, mig_cost, best_run.unbalanced_groups
