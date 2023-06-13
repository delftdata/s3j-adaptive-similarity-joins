import heapq
import time

from src.utils import cost_function, node_cost, penalty


def find_irremovable_big_groups(over, average):
    irremovable = []
    for node in over.keys():
        g = None
        for group in over[node]:
            if group[1] > average:
                if g is None:
                    g = group
                elif g[1] < group[1]:
                    g = group
        if g is not None:
            irremovable.append(g)
    return irremovable


def aaa_m(over, under, average):

    unbalanced = {**over, **under}
    overall = cost_function(unbalanced, average)
    ignore = []
    irremovable = find_irremovable_big_groups(over, average)
    count = 0
    mig_cost = 0

    start_time = time.time()
    while 1:

        if time.time() - start_time > 1800:
            print("AAA: Exceeded time limit.")
            break

        count += 1
        over_benefits = []
        best = None
        for node in over.keys():
            nc = node_cost(over[node])
            pen = penalty(nc, average)
            for group in over[node]:
                group_benefit = pen - penalty(nc - group[1], average) - 0.1 * group[-1]
                if group_benefit > 0 and group not in irremovable:
                    over_benefits.append((-group_benefit, group, node))

        heapq.heapify(over_benefits)
        if over_benefits:
            best = heapq.heappop(over_benefits)

        while best in ignore:
            if over_benefits:
                best = heapq.heappop(over_benefits)
            else:
                best = None

        if best is None:
            break

        optimal = None
        under_benefits = []
        for u_node in under:
            nc = node_cost(under[u_node])
            pen = penalty(nc, average)
            node_benefit = pen - penalty(nc + best[1][1], average)
            under_benefits.append((-node_benefit, u_node))

        heapq.heapify(under_benefits)
        while under_benefits:
            optimal = heapq.heappop(under_benefits)
            if optimal is None or node_cost(under[optimal[1]]) < average:
                break
            else:
                optimal = None

        if optimal is None:
            ignore.append(best)
        else:
            over[best[2]].remove(best[1])
            under[optimal[1]].append(best[1])
            mig_cost += best[1][-1]

        overall = cost_function(unbalanced, average)

    return overall, mig_cost, unbalanced
