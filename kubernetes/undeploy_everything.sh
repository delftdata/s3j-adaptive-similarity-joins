kubectl delete deploy/my-first-flink-cluster -n flink
kubectl delete deploy/coordinator -n flink
kubectl delete svc/coordinator -n flink

helm uninstall kafka -n kafka
helm uninstall minio -n flink

kubectl delete pvc/data-0-minio-0 -n flink
kubectl delete pvc/data-0-minio-1 -n flink
kubectl delete pvc/data-1-minio-0 -n flink
kubectl delete pvc/data-1-minio-1 -n flink
kubectl delete pvc/data-kafka-0 -n kafka
kubectl delete pvc/data-kafka-zookeeper-0 -n kafka
kubectl delete namespace/kafka

