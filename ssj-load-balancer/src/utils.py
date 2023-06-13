from collections import defaultdict


def node_cost(groups):
    node_total = 0
    for item in groups:
        node_total += item[1]
    return node_total


def penalty(cost, average):
    return abs(cost - average)


def cost_function(unbalanced, average):
    total = 0
    for node in unbalanced.keys():
        total += penalty(node_cost(unbalanced[node]), average)

    return total


def sorting_func(item):
    return item[1]


def over_under_classification(unbalanced_nodes, average):
    over = defaultdict(list)
    under = defaultdict(list)
    for node in unbalanced_nodes.keys():
        nc = node_cost(unbalanced_nodes[node])
        if nc < average:
            under[node] = unbalanced_nodes[node]
        else:
            over[node] = unbalanced_nodes[node]

    return over, under


def update_sizes(gs: dict, ws: dict):
    for m in gs.keys():
        for key in ws.get(m).keys():
            if key in gs[m].keys():
                gs[m][key] = gs[m][key] + ws[m][key]
            else:
                gs[m][key] = ws[m][key]
