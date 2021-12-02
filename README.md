# onlinePartitioningForSsj

## Kubernetes Setup Instructions

### Requirements
- A Kubernetes cluster (or an emulator, e.g. [Minikube](https://minikube.sigs.k8s.io/docs/start/))
- A Flink distribution (can be the DELTA one) and a FLINK_HOME env var pointing to it
	- Put export `FLINK_HOME="/path/to/flink/"` in your `.bashrc`
- Docker

### Instructions for bringing cluster and dashboards up
1. Run `./kubernetes/start-everything.sh` 
2. Usually, this takes a while.. And Flink might not start after MinIO, which will cause it to break. If you see that the Flink deployment is all red in the dashboard, just run the Flink deployment script separately later (`./kubernetes/deploy-flink.sh`). Once MinIO is up, you can restart Flink without issue.
4. Open a new terminal, run `minikube tunnel`
5. Open a new terminal, run `./kubernetes/port-forward-minio.sh`. You can now visit MinIO's dashboard at `localhost:9000` (`minio, minio123`)

After this, you may either type `kubectl get svc` or go to the `services` category on the Kubernetes dashboard to get the relevant external cluster IPs for the dashboard of Flink (port 8081) and the Flask API. Minio's dashboard can be found under `localhost:9000` after the port-forward.

To reset a deployment, it is usually enough to just delete it and re-run its deployment script (e.g. for Flink and Flask at least this is the case). If you need to reset the entirety of Minikube, just start from step 1. again.

### API (port 5000)
- `<address>/setup`: sends the job and migration (currently placeholder) jars to Flink
- `<address>/start`: starts the distributed join job, it will throw an error but the job runs anyway if you check the dashboard
- `<address>/migrate`: (still WIP) stops the join job, creates a savepoint, then starts the migration job and waits for it to finish. Once it is finished, it resumes the join job.

### Docker Images
Currently the only relevant image is the one in the `coordinator` folder. For now, you can just build it whenever you change the API (`./coordinator/src/app.py`) and it should use the local version first. Beware, this works for Minikube only.
