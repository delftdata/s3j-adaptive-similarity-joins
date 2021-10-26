from kafka import KafkaConsumer
consumer = KafkaConsumer('my-topic')
for msg in consumer:
	print (msg)