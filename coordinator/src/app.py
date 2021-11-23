from flask import Flask
import requests
import json
import time
import os
from flask import jsonify

app = Flask(__name__)

SAVEPOINT_IN_PROGRESS_STATUS = "IN_PROGRESS"
SAVEPOINT_COMPLETED_STATUS = "COMPLETED"

MIGRATION_JAR_PATH = "/jars/WordCount.jar"
JOB_JAR_PATH = "/jars/online_partitioning_for_ssj-1.0.jar"

BASE_URL = "http://my-first-flink-cluster-rest:8081"
KAFKA_URL = "http://kafka-cluster-kafka-bootstrap.kafka:9092"
FS_PATH = "s3://flink/savepoints"

save_point_path = ""
job_ids = {}


@app.route('/')
def hello():
    return 'Hello world'

@app.route('/stop-and-create-savepoint')
def stop_and_checkpoint():
    r = requests.get(f'{BASE_URL}/jobs')
    jobs = [j for j in r.json()["jobs"] if j["status"]=="RUNNING"]
    job_id = None if not jobs else jobs[0]['id']
    if job_id==None:
        return "No jobs to stop..."

    payload = {
            "targetDirectory" : FS_PATH,
            "drain": False
    }

    print("Stopping job: ", job_id)

    r = requests.post(f'{BASE_URL}/jobs/{job_id}/stop', data=json.dumps(payload), headers={'content-type': 'application/json'})
    print("REQ1 RES:")
    print(r.json())
    trigger_id = r.json()["request-id"]

    # Job stopping is an async operation, we need to query the status before we can continue
    status = SAVEPOINT_IN_PROGRESS_STATUS
    while status == SAVEPOINT_IN_PROGRESS_STATUS:
        r = requests.get(f'{BASE_URL}/jobs/{job_id}/savepoints/{trigger_id}')
        print("REQ2 RES - CHECKING:")
        print(r.json())
        status = r.json()["status"]["id"]
        time.sleep(0.5)

    if status == SAVEPOINT_COMPLETED_STATUS:
        global save_point_path
        print("REQ2 RES - FINAL:")
        print(r.json())
        save_point_path = r.json()["operation"]["location"]
        print("Current save point is located at: ", save_point_path)

    # RUN MIGRATION JOB AND WAIT FOR IT TO FINISH
    myheader = {'content-type': 'application/json'}
    data = {}#{"programArgsList": [save_point_path]}
    r = requests.post(f'{BASE_URL}/jars/{job_ids[MIGRATION_JAR_PATH]}/run', data=json.dumps(data), headers={'content-type': 'application/json'})
    print("REQ3 RES:")
    print(r.json())

    # QUERY JOB STATUS (shortcut: wait until there are no jobs)
    jobs = [None]
    while len(jobs) > 0:
        r = requests.get(f'{BASE_URL}/jobs')
        jobs = [j for j in r.json()["jobs"] if j["status"]=="RUNNING"]
        time.sleep(0.5)

    print("Migration completed, starting join job again...")

    return start_join_job()


# This route returns an error even though the job starts correctly
# Similar problem: https://stackoverflow.com/questions/58447465/flink-job-submission-fails-even-though-job-is-running
@app.route('/start-join-job')
def start_join_job():
    properties = {"programArgsList" : ["-kafkaURL", KAFKA_URL]}
    if save_point_path:
        properties["savepointPath"] = save_point_path
        print("Restarting join job with save point")
    r = requests.post(f'{BASE_URL}/jars/{job_ids[JOB_JAR_PATH]}/run', data=json.dumps(properties), headers={'content-type': 'application/json'})
    return r.json()


@app.route('/setup-jars')
def setup_jars():
    r = requests.get(f"{BASE_URL}/jars")
    files = r.json()["files"]
    R = []

    for file in files:
        print("Deleting job jar: ", file["id"])
        r = requests.delete(f"{BASE_URL}/jars/{file['id']}")
        R.append(r.json())

    for jar_path in [JOB_JAR_PATH, MIGRATION_JAR_PATH]:
        file = {"file" : (os.path.basename(jar_path), open(jar_path, "rb"), "application/x-java-archive")}
        upload = requests.post(f"{BASE_URL}/jars/upload", files=file)
        job_ids[jar_path] = os.path.basename(upload.json()["filename"])
        R.append(upload.json())

    return jsonify(R)

@app.route('/jobs')
def jobs():
    r = requests.get(f'{BASE_URL}/jobs')
    return r.json()


if __name__ == '__main__':
    print("Starting...")
    app.run(debug=True, host='0.0.0.0')



