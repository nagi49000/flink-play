import time
import logging
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.connectors import FlinkKafkaProducer
from pyflink.common.serialization import JsonRowDeserializationSchema
from pyflink.common.serialization import JsonRowSerializationSchema


logging.basicConfig(level=logging.DEBUG)


def iso_to_unix_secs(iso_str):
    """ convert an ISO datetime str to microsecs since unix epoch """
    dt = datetime.datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    usecs = time.mktime(dt.timetuple()) * 1000000 + dt.microsecond
    return usecs


def run_flink_time_transform():
    sleep_secs = 0  #debug
    logging.info(f"entering run_flink_time_transform and sleep for {sleep_secs} seconds")
    time.sleep(sleep_secs)
    logging.info(f"woken up after sleep for {sleep_secs} seconds")

    env = StreamExecutionEnvironment.get_execution_environment()
    logging.warning("adding jars")
    # don't know why I need to specify these jars manually... really should just be picked up automatically
    env.add_jars(
        "file:///opt/flink/opt/flink-connector-kafka_2.11-1.14.3.jar",
        "file:///opt/flink/opt/flink-connector-base-1.14.3.jar",
        "file:///opt/flink/opt/kafka-clients-3.1.0.jar"  # later version for log4j CVE
    )

    logging.warning(env)

    # set up the ingest from kafka
    # deserialize things JSON into {'time': '2022-02-05T15:20:09.429963Z'}
    deserialization_schema = JsonRowDeserializationSchema.builder().type_info(
        type_info=Types.ROW([Types.MAP(Types.STRING(), Types.STRING())])
    ).build()

    kafka_consumer = FlinkKafkaConsumer(
        topics='json-time-topic',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'kafka-flink-play:9092'}
    )

    data_stream = env.add_source(kafka_consumer)

    # perform streaming transform
    data_stream = data_stream.map(
        lambda x: {'usecs': iso_to_unix_secs(x['time'])},
        output_type=Types.MAP(Types.STRING(), Types.INT())
    )

    data_stream.print()  # debug

    # send to a new kafka topic
    # serialize to JSON
    serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
        type_info=Types.ROW([Types.MAP(Types.STRING(), Types.INT())])
    ).build()

    kafka_producer = FlinkKafkaProducer(
        topic='json-usecs-topic',
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': 'localhost:9092'}
    )

    data_stream.add_sink(kafka_producer)

    env.execute()

if __name__ == '__main__':
    run_flink_time_transform()
