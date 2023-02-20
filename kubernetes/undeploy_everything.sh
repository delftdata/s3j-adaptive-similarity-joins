kubectl delete deploy/my-first-flink-cluster 
kubectl delete deploy/coordinator 
kubectl delete svc/coordinator 

helm uninstall kafka -n kafka
helm uninstall minio 

kubectl delete pvc/data-0-minio-0 
kubectl delete pvc/data-0-minio-1 
kubectl delete pvc/data-1-minio-0 
kubectl delete pvc/data-1-minio-1 
kubectl delete pvc/data-kafka-0 -n kafka
kubectl delete pvc/data-kafka-zookeeper-0 -n kafka
kubectl delete namespace/kafka

