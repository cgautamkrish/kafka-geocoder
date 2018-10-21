from time import sleep
from json import dumps
from kafka import KafkaProducer

import geopy.distance
import geopy
import random
import math
import string
import jsonpickle

def id_generator(size=10, chars=string.ascii_uppercase + string.digits):
	return ''.join(random.choice(chars) for _ in range(size))

class Job:
	def __init__(self, left_bottom, left_top, right_bottom, right_top):
		self.id = id_generator()
		self.left_bottom = left_bottom
		self.left_top = left_top
		self.right_bottom = right_bottom
		self.right_top = right_top

PRODUCER = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))

def publish_job(job):
	partitions = [0,1,2]
	partition = random.choice(partitions)
	data = jsonpickle.encode(job)
	PRODUCER.send('jobs', value=data, partition=partition)
	print('job id ' + str(job.id) + ' assigned to partition ' + str(partition))

def box_boundaries(left_bottom_pt, right_top_pt, distance):
	dist = geopy.distance.VincentyDistance(meters=distance)
	left = dist.destination(point=left_bottom_pt, bearing=360)
	right = dist.destination(point=right_top_pt, bearing=180)
	return (geopy.Point(left.latitude, left.longitude), left_bottom_pt,
	 right_top_pt, geopy.Point(right.latitude, right.longitude))

def generate_jobs(left_lat=1.350, left_long=103.877, right_lat=1.354, right_long=103.882):
	bound_bottom = (left_lat, left_long)
	bound_top = (right_lat, right_long)

	distance = geopy.distance.vincenty(bound_bottom, bound_top).m
	# c^2 = 2a^2
	side_distance = math.sqrt((distance * distance)/2)
	left_bottom = geopy.Point(left_lat, left_long)
	right_top = geopy.Point(right_lat, right_long)

	new_pts = []
	while left_bottom.latitude < right_top.latitude:
		job_distance = geopy.distance.VincentyDistance(meters=5)
		temp = job_distance.destination(point=left_bottom, bearing=360)
		new_pts.append(temp)
		left_bottom = temp

	for job_pt in new_pts:
		job_distance = geopy.distance.VincentyDistance(meters=5)
		while job_pt.longitude < right_top.longitude:
			left = job_distance.destination(point=job_pt, bearing=90)
			new_left_bottom = geopy.Point(left.latitude, left.longitude)
			right = job_distance.destination(point=new_left_bottom, bearing=360)
			new_right_top = geopy.Point(right.latitude, right.longitude)
			
			box = box_boundaries(job_pt, new_right_top, 5)
			job_pt = new_left_bottom

			new_job = Job(box[1], box[0], box[3], box[2])
			publish_job(new_job)
			del new_job

generate_jobs()
