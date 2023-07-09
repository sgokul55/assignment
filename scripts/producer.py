import socket
import time
from confluent_kafka import Producer

conf = {'bootstrap.servers': "localhost:9092", 'client.id': socket.gethostname()}
producer = Producer(conf)
e = ["R_", "G_", "B_"]
max_nr_event_ingestion = 300000
burst_count = 10000
event_delay = 10

def alternate_event_ingestion():
    for x in range(max_nr_event_ingestion):
        producer.produce("events", key=str(time.time()), value=e[x % 3] + str(int(time.time()) + event_delay + (x % 3)))

def bursty_event_ingestion():
    count = 0
    e_index = 0
    for x in range(max_nr_event_ingestion):
        if count < burst_count:
            producer.produce("events", key=str(time.time()), value=e[e_index] + str(int(time.time()) + event_delay))
            count += 1
        else:
            count = 0
            e_index = (e_index + 1) % 3

def out_of_order_ingestion():
    for x in range(max_nr_event_ingestion):
        r = value=e[0] + str(int(time.time()) + event_delay)
        g = value=e[1] + str(int(time.time()) + event_delay + 1)
        b = value=e[2] + str(int(time.time()) + event_delay + 2)
        producer.produce("events", key=str(time.time()), value= g)
        producer.produce("events", key=str(time.time()), value= b)
        producer.produce("events", key=str(time.time()), value= r)
        count += 1
