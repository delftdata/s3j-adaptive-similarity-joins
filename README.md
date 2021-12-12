# onlinePartitioningForSsj

## Submodules

This project has two submodules:
- Coordinator
- Monitor

To initialize them properly, perform the following two commands after cloning:
```
git submodule init
git submodule update
```

## Kubernetes Minikube Setup Instructions

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
- `<address>/stop`: stops the join job and creates a savepoint
- `<address>/migrate`: starts the migration job and waits for it to finish. Once it is finished, it resumes the join job.
- `<address>/jobs`: gets the jobs that are currently running, mostly for debug

### Docker Images
Currently the only relevant image is the one in the `coordinator` folder. For now, you can just build it whenever you change the API (`./coordinator/src/app.py`) and it should use the local version first. Beware, this works for Minikube only.



## K3s cluster instructions

**IMPORTANT:** Run ALL commands with `sudo`!!

### Managing the deployments
The `kubernetes` folder has a `redeploy-*.sh` script for every deployment, which takes the deployment down and restarts it. This should in principle completely reset that particular deployment.

If nothing is running yet, use `start-everything.sh` to bring all deployments up.

### Checking deployment status
You can use the script that is provided, after starting everything: `check-deployments.sh`

You can also inspect further with the following commands:
- `sudo k3s kubectl get deployments --all-namespaces`
- `sudo k3s kubectl get pods --all-namespaces`
- `sudo k3s kubectl get svc --all-namespaces`

et cetera.

### Getting the addresses in order to interact with the services
To see which ports and addresses can be used to connect to externally, use `sudo k3s kubectl get all svc` to view the addresses, specifically you want the ones under `EXTERNAL-IP`. Some services may show up to 4 addresses, in such cases you can use any of them.

### Change docker images
In order to change the images used for the coordinator and the stat monitor, check the deployment files:
- coordinator: `./kubernetes/deployments/coordinator.yaml`
- stat monitor: `./kubernetes/deployments/monitor.yaml`

In both of them, look for the `image:` property, and replace that with a reference to your own version of the image.

You should be able to build the images from the `stat monitor` and `coordinator` repositories and push them under your own username on dockerhub, in case you need to make adjustments (e.g. to the algorithm used for the stat monitor).


### Managing the Flink jobs
The API is the same as above, except now you need to do curl requests to the external IP of the coordinator service, e.g.:
```
curl http://<COORDINATOR_IP>:5000/setup
```

Remember to first call `setup` in case the flink cluster is fresh, so it has the jars needed for the jobs. Then you may call the `start` route to start the join job.