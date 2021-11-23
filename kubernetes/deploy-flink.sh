#!/usr/bin/env bash
kubectl create clusterrolebinding flink-role-binding-default --clusterrole=edit --serviceaccount=default:default
$FLINK_HOME/bin/kubernetes-session.sh \
    -Dkubernetes.cluster-id=my-first-flink-cluster \
    -Dkubernetes.container.image=flink:1.12.5-scala_2.12-java11 \
    -Ds3.endpoint=minio:9000 \
	-Ds3.path-style=true \
	-Ds3.access-key=minio \
	-Ds3.secret-key=minio123 \
	-Dtaskmanager.numberOfTaskSlots=10 \
	-Dcontainerized.master.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.12.5.jar \
    -Dcontainerized.taskmanager.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.12.5.jar
