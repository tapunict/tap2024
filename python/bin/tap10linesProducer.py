# https://towardsdatascience.com/kafka-python-explained-in-10-lines-of-code-800e3e07dad
import os
from time import sleep
from json import dumps
from kafka import KafkaProducer

topic = os.getenv("KAFKA_TOPIC", "tap")
pause = int(os.getenv("pause", 5))
producer = KafkaProducer(bootstrap_servers=['kafkaServer:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

e=0
while(True):
    data = {'number' : e}
    producer.send(topic, value=data)
    e+=1
    sleep(pause)
