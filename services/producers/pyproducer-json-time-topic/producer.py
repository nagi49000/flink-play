from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime
from time import sleep
import logging
import json


logging.basicConfig(level=logging.DEBUG)
sleep(10)  # sleep whilst other services come up

producer = KafkaProducer(
    bootstrap_servers=["127.0.0.1:29092"], retries=3,
    value_serializer=lambda m: json.dumps(m).encode("ascii")
)

# launch a limited number of messages
for _ in range(10):
    producer.send("json-time-topic", {"time": datetime.utcnow().isoformat() + "Z"})
    sleep(3)
producer.flush()
