from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime
from time import sleep
import logging
import json


logging.basicConfig(level=logging.DEBUG)
sleep(10)  # sleep whilst other services come up

# hostname in bootstrap_servers obtained from link in docker-compose.yml
producer = KafkaProducer(
    bootstrap_servers=["kafka-flink-play:9092"], retries=3,
    value_serializer=lambda m: json.dumps(m).encode("ascii")
)

# launch a limited number of messages
for _ in range(5):
    producer.send("json-time-topic", {"time": datetime.utcnow().isoformat() + "Z"})
    sleep(3)
producer.flush()
producer.close(timeout=5)
