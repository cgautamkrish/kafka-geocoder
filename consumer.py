from kafka import KafkaConsumer
from json import loads

import string
import random
import jsonpickle

def group_id_generator(size=5, chars=string.ascii_uppercase + string.digits):
	return ''.join(random.choice(chars) for _ in range(size))

# group_id = group_id_generator()
CONSUMER = KafkaConsumer('jobs',
	bootstrap_servers = ['localhost:9092'],
	auto_offset_reset = 'earliest',
	enable_auto_commit = True,
	group_id = 'group-one',
	value_deserializer = lambda x: loads(x.decode('utf-8')))

for message in CONSUMER:
	message = message.value
	job = jsonpickle.decode(message)
	print('Received job with id:' + str(job['id']))