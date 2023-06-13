import argparse
import configparser
import os


# Method to parse command-line arguments and a configuration file. It returns an arguments and a configuration object.
def setup():
    arg_parser = argparse.ArgumentParser()
    arg_group = arg_parser.add_mutually_exclusive_group()
    arg_group.add_argument("-a", "--all", help="Use all available load balancing algorithms", action="store_true")
    arg_group.add_argument("-algo", "--algorithms",
                           help="Pick the desired algorithm(s), "
                                "choices = \"[aaa, aaa_m, mr-partition, greedy, imlp_m, imlp_c]\"",
                           choices=["aaa", "aaa_m", "mr-partition", "greedy", "imlp_m", "imlp_c"],
                           nargs='+',
                           action="store")
    arg_metric_group = arg_parser.add_mutually_exclusive_group()
    arg_metric_group.add_argument("-av", "--average", help="Use the average latencies per machine as a metric.",
                                  action="store_true")
    arg_metric_group.add_argument("-p", "--percentiles", help="Choose a latency percentile as a metric.",
                                  choices=["50%", "90%", "95%", "99%", "99.9%"],
                                  nargs=1,
                                  action="store")
    arg_parser.add_argument("-c", "--config",
                            help="Provide a configuration file.",
                            default="/resources/config.ini",
                            action="store")
    arg_parser.add_argument("-mp", "--multiprocessing",
                            help="Choose if algorithms should run in parallel or not.",
                            action="store_true")
    arg_parser.add_argument("-m", "--minimize",
                            help="Choose the metric to minimize. [load_imbalance, migration, runtime]",
                            choices=["load_imbalance", "migration", "runtime"],
                            default="load_imbalance",
                            action="store")
    arguments = arg_parser.parse_args()

    configuration = configparser.ConfigParser()
    configuration.read(os.getcwd()+arguments.config)

    return arguments, configuration
