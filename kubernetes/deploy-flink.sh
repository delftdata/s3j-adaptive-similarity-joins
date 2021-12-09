#!/usr/bin/env bash
k3s kubectl create clusterrolebinding flink-role-binding-default --clusterrole=edit --serviceaccount=default:default
$FLINK_HOME/bin/kubernetes-session.sh \
    -Dkubernetes.cluster-id=my-first-flink-cluster \
    -Dkubernetes.container.image=archer6621/flink:1.12.1-delta \
    -Ds3.endpoint=http://$(k3s kubectl get svc | grep 'minio ' | awk '{print $3}'):9000 \
	-Ds3.path-style=true \
	-Ds3.access-key=minio \
	-Ds3.secret-key=minio123 \
	-Dtaskmanager.numberOfTaskSlots=10 \
	-Dcontainerized.master.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.12.5.jar \
    -Dcontainerized.taskmanager.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.12.5.jar
k3s kubectl patch deployment my-first-flink-cluster --type json -p '[{"op": "add", "path": "/spec/template/spec/containers/0/envFrom", "value": [{"configMapRef": {"name": "env-config"}}] }]'
