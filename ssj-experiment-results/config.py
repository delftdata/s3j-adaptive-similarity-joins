import argparse
import os


def parseArguments():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-k", "--kafka",
                            help="Provide kafka bootstrap server, ip/hostname:port",
                            default="localhost:9092",
                            action="store")
    arg_parser.add_argument("-t", "--topic",
                            help="Provide the topic containing the metrics.",
                            default="pipeline-out-stats",
                            action="store")
    arg_parser.add_argument("-e", "--end",
                            help="Provide the offset of the last data to read.",
                            default=None,
                            action="store")
    arg_parser.add_argument("-n", "--name",
                            help="Provide the name of the experiment.",
                            default=None,
                            action="store")
    arg_parser.add_argument("-c", "--coordinator",
                            help="Provide the name of the coordinator's pod.",
                            default=None,
                            action="store")
    arg_parser.add_argument("-l", "--location",
                            help="Provide the location where the files should be stored",
                            default=os.path.join(os.path.dirname(os.path.abspath(__file__))),
                            action="store")

    return arg_parser.parse_args()


def parseDrawArguments():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-n", "--name",
                            help="Provide the name of the experiment.",
                            default=None,
                            action="store")
    arg_parser.add_argument("-t", "--type",
                            help="Provide the type of the experiment.",
                            default=None,
                            action="store")
    arg_parser.add_argument("-l", "--location",
                            help="Provide the location where the files should be stored",
                            default=os.path.join(os.path.dirname(os.path.abspath(__file__))),
                            action="store")
    arg_parser.add_argument("-p", "--paper",
                            default=False,
                            action="store_true")

    return arg_parser.parse_args()


def parseFlinkMetrics():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-om", "--operator_metrics",
                            help="Provide the metrics to retrieve from Flink for a specific operator.",
                            default=None,
                            action="store")

    arg_parser.add_argument("-tm", "--taskmanager_metrics",
                            help="Provide the metrics to retrieve from Flink for a taskmanager.",
                            default=None,
                            action="store")

    arg_parser.add_argument("-en", "--experiment_name",
                            help="Provide a name for the experiment.",
                            default=None,
                            action="store",
                            required=True)

    # arg_parser.add_argument("-id", "--job_id",
    #                         help="Provide the jobID.",
    #                         default=None,
    #                         action="store",
    #                         required=True)

    return arg_parser.parse_args()


