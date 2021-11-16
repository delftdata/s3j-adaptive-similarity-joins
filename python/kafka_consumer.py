from kafka import KafkaConsumer
consumer = KafkaConsumer('pipeline-out-stats', bootstrap_servers='localhost:9092', auto_offset_reset='earliest', group_id=None)

machine_level_computations_flag = False
group_level_computations_flag = False
machine_level_computations_info = None
group_level_computations_info = None

for msg in consumer:
	if msg.key.decode() == "final-comps-per-machine":
		machine_level_computations_flag = True
		machine_level_computations_info = msg.value.decode()
	elif msg.key.decode() == "final-comps-per-group":
		group_level_computations_flag = True
		group_level_computations_info = msg.value.decode()

	if machine_level_computations_flag and group_level_computations_flag:
		print("Final Computations per Machine\n")
		print(machine_level_computations_info)
		print("\n")
		print("Final Computations per Group\n")
		print(group_level_computations_info)
		print("\n***************************************\n")
		machine_level_computations_flag = False
		group_level_computations_flag = False
		machine_level_computations_info = None
		group_level_computations_info = None
