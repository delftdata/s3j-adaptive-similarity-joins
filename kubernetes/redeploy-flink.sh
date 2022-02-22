#!/usr/bin/env bash

kubectl delete deploy my-first-flink-cluster
while [[ "$(kubectl get svc |(! grep "my-first-flink-cluster"))" ]]; do
        echo "Service still terminating... Waiting..."
        sleep 10
done

kubectl create clusterrolebinding flink-role-binding-default --clusterrole=edit --serviceaccount=default:default
$FLINK_HOME/bin/kubernetes-session.sh \
    -Dkubernetes.cluster-id=my-first-flink-cluster \
    -Dkubernetes.container.image=gsiachamis/flink:1.12.1-delta \
    -Dstate.backend=rocksdb \
    -Dstate.checkpoints.dir=s3://flink/checkpoints \
    -Ds3.endpoint=http://$(kubectl get svc | grep 'minio ' | awk '{print $3}'):9000 \
    -Ds3.path-style=true \
    -Ds3.access-key=minio \
    -Ds3.secret-key=minio123 \
    -Dblob.server.port=6124 \
    -Dtaskmanager.rpc.port=6122 \
    -Dtaskmanager.numberOfTaskSlots=1 \
    -Dtaskmanager.memory.process.size=8000m \
    -Djobmanager.memory.process.size=8000m \
    -Dcontainerized.master.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.12.1-DELTA.jar \
    -Dcontainerized.taskmanager.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.12.1-DELTA.jar
kubectl patch deployment my-first-flink-cluster --type json -p '[{"op": "add", "path": "/spec/template/spec/containers/0/envFrom", "value": [{"configMapRef": {"name": "env-config"}}] }]'