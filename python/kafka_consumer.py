from kafka import KafkaConsumer
consumer = KafkaConsumer('pipeline-out-stats', bootstrap_servers='localhost:9092', auto_offset_reset='earliest', group_id=None)
for msg in consumer:
	print (msg.value.decode())