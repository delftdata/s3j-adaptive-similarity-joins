# onlinePartitioningForSsj

# Set up modified Flink

- Get the jar from here: https://surfdrive.surf.nl/files/index.php/s/qGBEpsokMOrabVr
- Put the jar in the root directory of the project and run the following command:

```
mvn install:install-file -DlocalRepositoryPath=libs -DcreateChecksum=true -Dpackaging=jar -Dfile=flink-dist_2.11-1.12.1.jar -DgroupId=org.apache.flink -DartifactId=flink-dist -Dversion=1.12.1
```

# Kubernetes Setup Instructions

## Requirements
- A Kubernetes cluster (or an emulator, e.g. [Minikube](https://minikube.sigs.k8s.io/docs/start/))
- A Flink distribution (can be the DELTA one) and a FLINK_HOME env var pointing to it
	- Put export `FLINK_HOME="/path/to/flink/"` in your `.bashrc`

## Instructions for bringing cluster and dashboards up
1. Run `./kubernetes/minikube-restart.sh` 
2. Open a new terminal, run `minikube dashboard`
3. Start all the other deployments via the provided scripts in `./kubernetes`
4. Open a new terminal, run `minikube tunnel`
5. Open a new terminal, run `kubectl port-forward service/minio 9000`

After this, you may either type `kubectl get svc` or go to the `services` category on the Kubernetes dashboard to get the relevant external cluster IPs for the dashboard of Flink (port 8081) and the Flask API. Minio's dashboard can be found under `localhost:9000` after the port-forward.

To reset a deployment, it is usually enough to just delete it and re-run its deployment script (e.g. for Flink and Flask at least this is the case). If you need to reset the entirety of Minikube, just start from step 1. again.

## API (port 5000)
- `<address>/setup-jars`: sends the job and migration (currently placeholder) jars to Flink
- `<address>/start-join-job`: starts the distributed join job, it will throw an error but the job runs anyway if you check the dashboard
- `<address>/stop-and-create-savepoint`: (still WIP) stops the join job, creates a savepoint, then starts the migration job and waits for it to finish. Once it is finished, it resumes the join job.

## Docker Images
Currently the only relevant image is the one in the `coordinator` folder. Will probably use a proper Flask image in the future. For now, hopefully you can just build it whenever you change the API (`,/coordinator/src/app.py` and it should use the local version first. If not, perhaps renaming the image helps.
