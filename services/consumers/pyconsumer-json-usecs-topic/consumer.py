import time
import json
from kafka import KafkaConsumer
import logging
logging.basicConfig(level=logging.INFO)

# sleep whilst kafka stands up
sleep_secs = 5
logging.info(f"sleeping for {sleep_secs} secs")
time.sleep(sleep_secs)
logging.info(f"woke up after {sleep_secs} secs")

json_consumer = KafkaConsumer("json-usecs-topic",
                              bootstrap_servers=["kafka-flink-play:9092"],
                              value_deserializer=lambda m: json.loads(m.decode('ascii')),
                              enable_auto_commit=False,
                              auto_offset_reset='earliest')

# this loop runs forever
for message in json_consumer:
    logging.info(message.value)
