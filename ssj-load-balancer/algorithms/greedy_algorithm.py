from src.utils import cost_function, sorting_func
import time


def repartitioning(unbalanced, unbalanced_groups, k, num_of_groups):
    if k > num_of_groups:
        k = num_of_groups
    removed = []
    mig_cost = 0
    time_flag = False

    start_time = time.time()

    while k > 0:

        if time.time() - start_time > 1800:
            print("Greedy exceeded time limit.")
            time_flag = True
            break

        unbalanced.sort(reverse=True, key=sorting_func)
        max_loaded = unbalanced[0]
        if unbalanced_groups[max_loaded[0]] == []:
            break
        removed_job = unbalanced_groups[max_loaded[0]][0]
        unbalanced_groups[max_loaded[0]].remove(removed_job)
        removed.append([max_loaded[0], removed_job])
        unbalanced[0][1] = unbalanced[0][1] - removed_job[1]
        k -= 1

    if time_flag:
        return None, None, None

    while removed:

        if time.time() - start_time > 1800:
            print("Greedy exceeded time limit.")
            time_flag = True
            break

        unbalanced.sort(key=sorting_func)
        min_loaded = unbalanced[0]
        unbalanced_groups[min_loaded[0]].append(removed[0][1])
        unbalanced[0][1] = unbalanced[0][1] + removed[0][1][1]
        if min_loaded[0] != removed[0][0]:
            mig_cost += removed[0][1][-1]
        removed.remove(removed[0])

    if time_flag:
        return None, None, None

    return unbalanced, unbalanced_groups, mig_cost


def greedy_2app(unbalanced, unbalanced_groups, average):

    num_of_groups = 0
    for node in unbalanced_groups.keys():
        num_of_groups += len(unbalanced_groups[node])
        unbalanced_groups[node].sort(reverse=True, key=sorting_func)

    num_of_moves = num_of_groups / 2
    if num_of_moves > 10000:
        num_of_moves = 10000

    unbalanced_copy, unbalanced_groups, mig_cost = repartitioning(unbalanced, unbalanced_groups, num_of_moves,
                                                                  num_of_groups)

    if unbalanced_copy is None:
        return None, None

    return cost_function(unbalanced_groups, average), mig_cost, unbalanced_groups
