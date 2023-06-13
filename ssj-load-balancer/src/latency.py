import itertools


def get_average_latency(sum, count):
    if count == 0:
        return None
    return sum / count


class AverageLatency:

    def __init__(self, sum, count):
        self.sum = sum
        self.count = count

    def get_average_latency(self):
        if self.count == 0:
            return None
        return self.sum/self.count


class LatencyMonitoring:

    def __init__(self, constraints):
        self.constraints = constraints

    def check_latency_constraints(self, metric, args):
        if "percentage" in self.constraints.keys():
            p = self.constraints["percentage"]
            if(args.average):
                return self.use_average_latencies(metric, p)
            else:
                return self.use_percentiles(metric, p, args.percentiles[0])
        else:
            return False

    def use_percentiles(self, percentiles, p, perc_key):
        print(perc_key)
        for mac1, mac2 in itertools.combinations(percentiles.keys(), 2):
            if percentiles[mac1] is None:
                percentiles[mac1] = {}
                percentiles[mac1][perc_key] = 0
            if percentiles[mac2] is None:
                percentiles[mac2] = {}
                percentiles[mac2][perc_key] = 0
            if percentiles[mac1][perc_key] is None or percentiles[mac2][perc_key] is None:
                return True
            elif (percentiles[mac1][perc_key] > (1 + p) * percentiles[mac2][perc_key]
                  or percentiles[mac1][perc_key] < (1 - p) * percentiles[mac2][perc_key]):
                return True
        return False

    def use_average_latencies(self, latencies, p):
        average_system_latency = self.average_system_latency(latencies)
        for mac in latencies.keys():
            av_latency = get_average_latency(latencies[mac][0], latencies[mac][1])
            if av_latency is None:
                return True
            if (av_latency > (1 + p) * average_system_latency
                    or av_latency < (1 - p) * average_system_latency):
                return True

        return False

    def average_system_latency(self, latencies):
        s = 0
        c = 0
        for mac in latencies.keys():
            s += latencies[mac][0]
            c += latencies[mac][1]
        return s/c

    # def reset_latencies(self, average_latencies):
    #     for m in average_latencies:
    #         average_latencies[m].sum = 0
    #         average_latencies[m].count = 0
