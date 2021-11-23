from kafka import KafkaConsumer
consumer = KafkaConsumer('pipeline-out')
for msg in consumer:
	print (msg)