import io
import json

import minio

from load_rebalancing import Rebalancer
from main import create_dummy_example
from setup import setup

machines = [0, 13, 26, 39, 52, 65, 78, 91, 104, 117]

group_keys = [
    (1, 0, 0),
    (2, 0, 0),
    (2, 13, 13),
    (1, 13, 13),
    (1, 26, 26),
    (2, 26, 26),
    (1, 39, 39),
    (3, 39, 39),
    (2, 39, 39),
    (2, 52, 52),
    (1, 52, 52),
    (1, 65, 65),
    (2, 65, 65),
    (2, 78, 78),
    (3, 78, 78),
    (1, 78, 78),
    (1, 91, 91),
    (1, 104, 104),
    (2, 104, 104),
    (1, 117, 117),
    (2, 117, 117)
]

if __name__ == '__main__':

    args, config = setup()

    old_assignment, average = create_dummy_example()

    if args.all:
        algorithms = ["aaa", "aaa_m", "mr-partition", "greedy", "imlp_m", "imlp_c"]
    else:
        algorithms = args.algorithms

    rebalancer = Rebalancer(algorithms, args.minimize, config)
    algo, result = rebalancer.create_new_assignment(old_assignment, average)
    print("algorithm: %s, load_imbalance: %d, migration: %d, runtime %f" %
          (algo, result.load_imbalance, result.migration, result.runtime))

    group_2_node_dict = dict()
    for node in result.assignment.keys():
        for group in result.assignment.get(node):
            group_2_node_dict[str(group[0])] = node


    minio_client = minio.Minio(
        config["Minio"]["hosts"],
        access_key=config["Minio"]["access_key"],
        secret_key=config["Minio"]["secret_key"],
        secure=False
    )

    if not minio_client.bucket_exists(config["Minio"]["bucket"]):
        minio_client.make_bucket(config["Minio"]["bucket"])

    alloc = json.dumps(group_2_node_dict)
    minio_client.put_object(config["Minio"]["bucket"],
                            "alloc_example.txt",
                            data=io.BytesIO(alloc.encode('utf-8')),
                            length=len(alloc))

    print("Allocation file is written in MinIO.")
