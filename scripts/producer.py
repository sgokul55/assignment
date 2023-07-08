import socket
import time
from confluent_kafka import Producer

conf = {'bootstrap.servers': "localhost:9092", 'client.id': socket.gethostname()}
producer = Producer(conf)

e = ["R_", "G_", "B_"]
for x in range(300000):
    producer.produce("events", key=str(time.time()), value=e[x % 3] + str(int(time.time())) )
