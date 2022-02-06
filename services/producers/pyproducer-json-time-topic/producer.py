from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime
from time import sleep
import logging
import json


logging.basicConfig(level=logging.INFO)
sleep(5)  # sleep whilst other services come up

# hostname in bootstrap_servers obtained from link in docker-compose.yml
producer = KafkaProducer(
    bootstrap_servers=["kafka-flink-play:9092"], retries=3,
    value_serializer=lambda x: json.dumps(x).encode("ascii")
)

# launch a limited number of messages
for _ in range(5):
    m = {"time": datetime.utcnow().isoformat() + "Z"}
    producer.send("json-time-topic", m)
    logging.info(f"sent message {m}")
    sleep(3)
producer.flush()
producer.close(timeout=5)
