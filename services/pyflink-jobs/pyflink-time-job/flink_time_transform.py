import json
import time
import logging
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.connectors import FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema


logging.basicConfig(level=logging.DEBUG)


def iso_to_unix_secs(s):
    """ convert an json str like {'time': '2022-02-05T15:20:09.429963Z'} to microsecs since unix epoch
        as a json str {'usecs': 164000980084}
    """
    dt_str = json.loads(s)["time"]
    dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    usecs = time.mktime(dt.timetuple()) * 1000000 + dt.microsecond
    return json.dumps({"usecs": usecs})


def run_flink_time_transform():
    sleep_secs = 0  #debug
    logging.info(f"entering run_flink_time_transform and sleep for {sleep_secs} seconds")
    time.sleep(sleep_secs)
    logging.info(f"woken up after sleep for {sleep_secs} seconds")

    env = StreamExecutionEnvironment.get_execution_environment()  # this call resets the log level to WARNING
    logging.warning("adding jars")
    # don't know why I need to specify these jars manually... really should just be picked up automatically
    env.add_jars(
        "file:///opt/flink/opt/flink-connector-kafka_2.11-1.14.3.jar",
        "file:///opt/flink/opt/flink-connector-base-1.14.3.jar",
        "file:///opt/flink/opt/kafka-clients-3.1.0.jar"  # later version for log4j CVE
    )

    logging.warning(env)

    # set up the ingest from kafka
    # contains JSON like {'time': '2022-02-05T15:20:09.429963Z'}
    kafka_consumer = FlinkKafkaConsumer(
        topics='json-time-topic',
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': 'kafka-flink-play:9092'}
    )

    data_stream = env.add_source(kafka_consumer)

    # perform streaming transform
    data_stream = data_stream.map(
        lambda x: iso_to_unix_secs(x),
        output_type=Types.STRING()
    )

    data_stream.print()  # debug

    # send to a new kafka topic
    kafka_producer = FlinkKafkaProducer(
        topic='json-usecs-topic',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka-flink-play:9092'}
    )

    data_stream.add_sink(kafka_producer)

    env.execute("pyFlink timestamp to microsecs job")

if __name__ == '__main__':
    run_flink_time_transform()
