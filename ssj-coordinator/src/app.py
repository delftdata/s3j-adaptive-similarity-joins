from flask import Flask, request, Response
import requests
import json
import time
import os
from flask import jsonify
from minio import Minio
from kafka import KafkaProducer
from google.cloud import storage

app = Flask(__name__)

SAVEPOINT_IN_PROGRESS_STATUS = "IN_PROGRESS"
SAVEPOINT_COMPLETED_STATUS = "COMPLETED"

MIGRATION_JAR_PATH = "/jars/state_migration_job-1.0.jar"
JOB_JAR_PATH = "/jars/online_partitioning_for_ssj-1.0.jar"
GENERATOR_JAR_PATH = "/jars/StreamGenerator-1.0.jar"
STATS_JAR_PATH = "/jars/ssj-statistics-1.0.jar"
CJ_JAR_PATH = "/jars/clusterjoin-based-baseline-1.0.jar"
CJSTATS_JAR_PATH = "/jars/cj-stats-1.0.jar"

MINIO_URL = f"{os.environ['MINIO_ADDRESS']}"
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
BASE_URL = 'http://' + os.environ["FLINK_ADDRESS"]
KAFKA_URL = 'http://' + os.environ["KAFKA_ADDRESS"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
ALLOC_BUCKET_NAME = os.environ["ALLOC_BUCKET_NAME"]
EMBEDDINGS_BUCKET = os.environ["EMBEDDINGS_BUCKET"]

FS_PATH = f"s3://{BUCKET_NAME}/savepoints"

start_time = 0
save_point_path = ""
job_ids = {}
job_arguments = dict()
join_job_id = ""
migration_job_id = ""
stats_job_id = ""
cj_job_id = ""
cj_stats_id = ""
window_length = 1
parallelism = 1


@app.route('/')
def hello():
    return 'Hello world'


@app.route('/stop', methods=['POST'])
def stop_and_checkpoint():
    job_id_to_stop = request.json['job_id_to_stop']
    if (job_id_to_stop != join_job_id):
        print("The requested jobID doesn't match the id of the running job.")
        print("requested jobID: ", job_id_to_stop, ", running jobID: ", join_job_id)
        return Response(status=666)

    r = requests.get(f'{BASE_URL}/jobs/{join_job_id}')
    print(r.json()["jid"], " , ", r.json()["state"])
    if r.json()["state"] != "RUNNING":
        return "The join job is not running..."

    payload = {
        "targetDirectory": FS_PATH + '/' + str(join_job_id),
        "drain": False,
    }

    print("Stopping job: ", join_job_id)
    print("job run for :", time.time() - start_time)

    stop_start = time.time()
    r = requests.post(f'{BASE_URL}/jobs/{join_job_id}/stop', data=json.dumps(payload),
                      headers={'content-type': 'application/json'})
    print("REQ1 RES:")
    print(r.json())
    trigger_id = r.json()["request-id"]

    # Job stopping is an async operation, we need to query the status before we can continue
    status = SAVEPOINT_IN_PROGRESS_STATUS
    while status == SAVEPOINT_IN_PROGRESS_STATUS:
        r = requests.get(f'{BASE_URL}/jobs/{join_job_id}/savepoints/{trigger_id}')
        print("REQ2 RES - CHECKING:")
        print(r.json())
        status = r.json()["status"]["id"]
        time.sleep(10)

    if status == SAVEPOINT_COMPLETED_STATUS:
        global save_point_path
        print("REQ2 RES - FINAL:")
        print(r.json())
        if "location" in r.json()["operation"].keys():
            save_point_path = r.json()["operation"]["location"]
            print("Current save point is located at: ", save_point_path)
            print("Job was stopped successfully after " + str(time.time() - stop_start) + "s")
        else:
            print("Stopping job with a savepoint failed. Check logs...")
            print("Stopping job failed after " + str(time.time() - stop_start) + "s")
            return "Stopping job with a savepoint failed. Check logs..."

    # stats = requests.patch(f'{BASE_URL}/jobs/{stats_job_id}')
    # print("Stats cancellation: ", stats.status_code)

    return "Job stopped and savepoint stored!"


@app.route('/migrate', methods=['POST'])
def migrate_and_start():
    global migration_job_id, save_point_path
    print("Started migration")
    migration_start = time.time()
    allocation_filename = request.json["filename"]
    print("Migration file path: ", allocation_filename)

    # RUN MIGRATION JOB AND WAIT FOR IT TO FINISH
    state_backend_path = "/".join(save_point_path.split("/")[:-2])
    print("state_backend_path: " + state_backend_path)
    data = {"programArgsList": [
        "-minio", MINIO_URL,
        "-access", MINIO_ACCESS_KEY,
        "-secret", MINIO_SECRET_KEY,
        ALLOC_BUCKET_NAME, allocation_filename, save_point_path, state_backend_path]}
    r = requests.post(f'{BASE_URL}/jars/{job_ids[MIGRATION_JAR_PATH]}/run', data=json.dumps(data),
                      headers={'content-type': 'application/json'})
    print(r.json())
    migration_job_id = r.json()["jobid"]
    print("REQ3 RES:")
    print(r.json())

    # QUERY JOB STATUS (shortcut: wait until there are no jobs)
    while True:
        print("Waiting for migration to finish...")
        r = requests.get(f'{BASE_URL}/jobs/{migration_job_id}')
        migration_status = r.json()["state"]
        if migration_status == "FINISHED":
            break
        elif migration_status in ["FAILED", "CANCELED", "SUSPENDED"]:
            print("Job was cancelled or failed...")
            return "Job was cancelled or failed..."
        time.sleep(0.5)

    print("Migration completed, starting join job again...")
    print("Migration took " + str(time.time() - migration_start) + "s")
    save_point_path += "_updated"
    return start_join_job()


@app.route('/start', methods=['POST'])
def start_join_job():
    global join_job_id, start_time, stats_job_id, parallelism, window_length
    if save_point_path:
        properties = job_arguments[job_ids[JOB_JAR_PATH]]
        properties["savepointPath"] = save_point_path
        print("Restarting join job with save point " + save_point_path)
        args = properties["programArgsList"]
    else:
        args = request.json["args"].split(", ")
        properties = {"programArgsList": ["-kafkaURL", KAFKA_URL] + args}
        job_arguments[job_ids[JOB_JAR_PATH]] = properties

    try:
        parallelism = int(args[args.index("-parallelism") + 1])
    except ValueError:
        parallelism = 10

    try:
        window_length = int(args[args.index("-monitoringWindow") + 1])
    except ValueError:
        window_length = 120

    machines_id = []
    for group_id in range(parallelism):
        machines_id.append(int((group_id * 128 + parallelism - 1) / parallelism))

    r = requests.post(f'{BASE_URL}/jars/{job_ids[JOB_JAR_PATH]}/run', data=json.dumps(properties),
                      headers={'content-type': 'application/json'})
    join_job_id = r.json()["jobid"]
    print(join_job_id)
    start_time = time.time()
    producer = KafkaProducer(bootstrap_servers=os.environ["KAFKA_ADDRESS"])
    producer.send('pipeline-out-stats', key=b"running_job_id", value=join_job_id.encode())
    producer.send('pipeline-out-stats', key=b"machines-id", value=",".join(map(str, machines_id)).encode())

    return {"join_job_id": join_job_id}


@app.route('/start_stats')
def start_stats():
    global stats_job_id

    stats_parallelism = request.args.get("parallelism")
    args = ["-kafkaURL", KAFKA_URL,
            "-windowLength", window_length,
            "-jobID", join_job_id,
            "-parallelism", stats_parallelism
            ]
    stats_properties = {"programArgsList": args}
    stats = requests.post(f'{BASE_URL}/jars/{job_ids[STATS_JAR_PATH]}/run', data=json.dumps(stats_properties),
                          headers={'content-type': 'application/json'})
    stats_job_id = stats.json()["jobid"]

    return {"stats_job_id": stats_job_id}


@app.route('/start_stats_clusterjoin')
def start_stats_cj():
    global cj_stats_id

    cj_stats_parallelism = request.args.get("parallelism")
    args = ["-kafkaURL", KAFKA_URL,
            "-windowLength", window_length,
            "-jobID", cj_job_id,
            "-parallelism", cj_stats_parallelism
            ]
    cj_stats_properties = {"programArgsList": args}
    cj_stats = requests.post(f'{BASE_URL}/jars/{job_ids[CJSTATS_JAR_PATH]}/run', data=json.dumps(cj_stats_properties),
                          headers={'content-type': 'application/json'})
    cj_stats_id = cj_stats.json()["jobid"]

    return {"cj_stats_id": cj_stats_id}

@app.route('/setup')
def setup():
    # Send jars
    r = requests.get(f"{BASE_URL}/jars")
    files = r.json()["files"]
    R = []

    for file in files:
        print("Deleting job jar: ", file["id"])
        r = requests.delete(f"{BASE_URL}/jars/{file['id']}")
        R.append(r.json())

    for jar_path in [JOB_JAR_PATH, MIGRATION_JAR_PATH, GENERATOR_JAR_PATH, STATS_JAR_PATH, CJ_JAR_PATH, CJSTATS_JAR_PATH]:
        file = {"file": (os.path.basename(jar_path), open("." + jar_path, "rb"), "application/x-java-archive")}
        upload = requests.post(f"{BASE_URL}/jars/upload", files=file)
        job_ids[jar_path] = os.path.basename(upload.json()["filename"])
        R.append(upload.json())

    # Create bucket
    minio_client = Minio(MINIO_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)
    if not minio_client.bucket_exists(ALLOC_BUCKET_NAME):
        minio_client.make_bucket(ALLOC_BUCKET_NAME)
    if not minio_client.bucket_exists(EMBEDDINGS_BUCKET):

        gcs_client = storage.Client(project="abstract-robot-341415")
        gcs_bucket = gcs_client.bucket("ssj-material")
        embeddings_1k_blob = gcs_bucket.blob("wiki-news-300d-1K.vec")
        embeddings_1k_blob.download_to_filename("wiki-news-300d-1K.vec")
        embeddings_1m_blob = gcs_bucket.blob("wiki-news-300d-1M.vec")
        embeddings_1m_blob.download_to_filename("wiki-news-300d-1M.vec")
        imdb_dataset_10K_blob = gcs_bucket.blob("embeddingsIMDB_10K.txt")
        imdb_dataset_10K_blob.download_to_filename("imdb_dataset_10K.txt")
        imdb_dataset_100K_blob = gcs_bucket.blob("embeddingsIMDB_100K.txt")
        imdb_dataset_100K_blob.download_to_filename("imdb_dataset_100K.txt")
        imdb_dataset_1M_blob = gcs_bucket.blob("embeddingsIMDB_1Μ.txt")
        imdb_dataset_1M_blob.download_to_filename("imdb_dataset_1M.txt")

        minio_client.make_bucket(EMBEDDINGS_BUCKET)
        result = minio_client.fput_object(EMBEDDINGS_BUCKET, "1K_embeddings", "wiki-news-300d-1K.vec")
        R.append({"status": "object created", "created object": result.object_name, "etag:": result.etag,
                  "version-id": result.version_id})
        file1M = minio_client.fput_object(EMBEDDINGS_BUCKET, "1M_embeddings", "wiki-news-300d-1M.vec")
        R.append({"status": "object created", "created object": file1M.object_name, "etag:": file1M.etag,
                  "version-id": file1M.version_id})
        IMDB_dataset_1 = minio_client.fput_object(EMBEDDINGS_BUCKET, "imdb_dataset_10K", "imdb_dataset_10K.txt")
        R.append({"status": "object created", "created object": IMDB_dataset_1.object_name, "etag:": IMDB_dataset_1.etag,
                  "version-id": IMDB_dataset_1.version_id})

        IMDB_dataset_2 = minio_client.fput_object(EMBEDDINGS_BUCKET, "imdb_dataset_100K", "imdb_dataset_100K.txt")
        R.append({"status": "object created", "created object": IMDB_dataset_2.object_name, "etag:": IMDB_dataset_2.etag,
                  "version-id": IMDB_dataset_2.version_id})

        IMDB_dataset_3 = minio_client.fput_object(EMBEDDINGS_BUCKET, "imdb_dataset_1M", "imdb_dataset_1M.txt")
        R.append({"status": "object created", "created object": IMDB_dataset_3.object_name, "etag:": IMDB_dataset_3.etag,
                  "version-id": IMDB_dataset_3.version_id})
    else:
        result = minio_client.stat_object(EMBEDDINGS_BUCKET, "1K_embeddings")
        R.append({"status": "object existed", "created object": result.object_name, "etag:": result.etag,
                  "version-id": result.version_id, "last modified": result.last_modified})
        file1M = minio_client.stat_object(EMBEDDINGS_BUCKET, "1M_embeddings")
        R.append({"status": "object existed", "created object": file1M.object_name, "etag:": file1M.etag,
                  "version-id": file1M.version_id, "last modified": file1M.last_modified})
        IMDB_dataset_1 = minio_client.stat_object(EMBEDDINGS_BUCKET, "imdb_dataset_10K")
        R.append({"status": "object existed", "created object": IMDB_dataset_1.object_name, "etag:": IMDB_dataset_1.etag,
                  "version-id": IMDB_dataset_1.version_id, "last modified": IMDB_dataset_1.last_modified})
        IMDB_dataset_2 = minio_client.stat_object(EMBEDDINGS_BUCKET, "imdb_dataset_100K")
        R.append(
            {"status": "object existed", "created object": IMDB_dataset_2.object_name, "etag:": IMDB_dataset_2.etag,
             "version-id": IMDB_dataset_2.version_id, "last modified": IMDB_dataset_2.last_modified})
        IMDB_dataset_3 = minio_client.stat_object(EMBEDDINGS_BUCKET, "imdb_dataset_1M")
        R.append(
            {"status": "object existed", "created object": IMDB_dataset_3.object_name, "etag:": IMDB_dataset_3.etag,
             "version-id": IMDB_dataset_3.version_id, "last modified": IMDB_dataset_3.last_modified})

    return jsonify(R)


@app.route('/jobs')
def jobs():
    r = requests.get(f'{BASE_URL}/jobs')
    return r.json()


@app.route('/start_generator', methods=['POST'])
def start_generator_with_args():
    args = request.json["args"].split(", ")
    minio_args = ["-minioEndpoint", MINIO_URL, "-minioAccessKey", MINIO_ACCESS_KEY, "-minioSecretKey", MINIO_SECRET_KEY]
    properties = {"programArgsList": ["-kafkaURL", KAFKA_URL] + minio_args + args}
    r = requests.post(f'{BASE_URL}/jars/{job_ids[GENERATOR_JAR_PATH]}/run', data=json.dumps(properties),
                      headers={'content-type': 'application/json'})
    return r.json()


@app.route('/pwd')
def get_pwd():
    return os.getcwd()


@app.route('/ls')
def run_ls():
    print(os.popen('ls -a').readlines())
    return "OK"


@app.route('/get_join_jobid')
def get_join_jobid():
    return join_job_id


@app.route('/get_cj_jobid')
def get_cj_jobid():
    return cj_job_id


@app.route('/get_stats_jobid')
def get_stats_jobid():
    return stats_job_id


@app.route('/get_cj_stats_id')
def get_cj_stats_jobid():
    return cj_stats_id


@app.route('/reset_environment')
def reset_environment():
    global start_time, save_point_path, job_ids, job_arguments, join_job_id, migration_job_id, stats_job_id, cj_job_id, cj_stats_id
    stats_req = requests.patch(f'{BASE_URL}/jobs/{stats_job_id}')
    join_req = requests.patch(f'{BASE_URL}/jobs/{join_job_id}')
    cj_req = requests.patch(f'{BASE_URL}/jobs/{cj_job_id}')
    cj_stats_req = requests.patch(f'{BASE_URL}/jobs/{cj_stats_id}')
    start_time = 0
    save_point_path = ""
    job_ids = {}
    job_arguments = dict()
    join_job_id = ""
    migration_job_id = ""
    stats_job_id = ""
    cj_job_id = ""
    cj_stats_id = ""
    return "Experimental environment was successfully reset."


@app.route('/cancel_join_job')
def cancel_join_job():
    global join_job_id
    cancel_req = requests.patch(f'{BASE_URL}/jobs/{join_job_id}')
    return {"status_code": cancel_req.status_code}


@app.route('/cancel_stats_job')
def cancel_stats_job():
    global stats_job_id
    cancel_req = requests.patch(f'{BASE_URL}/jobs/{stats_job_id}')
    return {"status_code": cancel_req.status_code}


@app.route('/start_clusterjoin', methods=['POST'])
def start_clusterjoin_baseline():

    global cj_job_id, start_time, stats_job_id, parallelism, window_length

    args = request.json["args"].split(", ")
    properties = {"programArgsList": ["-kafkaURL", KAFKA_URL] + args}
    job_arguments[job_ids[CJ_JAR_PATH]] = properties

    try:
        parallelism = int(args[args.index("-parallelism") + 1])
    except ValueError:
        parallelism = 10

    try:
        window_length = int(args[args.index("-monitoringWindow") + 1])
    except ValueError:
        window_length = 120

    machines_id = []
    for group_id in range(parallelism):
        machines_id.append(int((group_id * 128 + parallelism - 1) / parallelism))


    r = requests.post(f'{BASE_URL}/jars/{job_ids[CJ_JAR_PATH]}/run', data=json.dumps(properties),
                      headers={'content-type': 'application/json'})
    cj_job_id = r.json()["jobid"]
    start_time = time.time()
    producer = KafkaProducer(bootstrap_servers=os.environ["KAFKA_ADDRESS"])
    producer.send('pipeline-out-stats', key=b"running_job_id", value=cj_job_id.encode())
    producer.send('pipeline-out-stats', key=b"machines-id", value=",".join(map(str, machines_id)).encode())

    return r.json()


if __name__ == '__main__':
    print("Starting...")
    app.run(debug=True, host='0.0.0.0')

