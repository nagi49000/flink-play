# flink-play
Simple examples using flink

The demo can be built running
```
# in services/
docker-compose build
```

and run with
```
# in services/
docker-compose up
```

The demo has as core infrastructure:
- a pyflink jobmanager
- a pyflink taskmanager
- zookeeper (for kafka)
- a single node kafka server

The kafka producers are:
- a python producer publishing records of the form {'time': '2022-02-07T23:26:17.272739Z'} to kafka topic 'json-time-topic'
- a python producer publishing records of the form {'name': 'james bond'} to kafka topic 'json-small-names-topic'

The kafka consumers are:
- a python consumer subscribing to records of the form {'usecs': 1644276377272739.0} from kafka topic 'json-usecs-topic'. Records are published to docker logs.

The flink jobs are:
- a pyflink job converting iso times to microseconds since unix epoch (submits job to jobmanager)

On running the demo, one should see the services come up, and the records flow through from producer, to taskmanager and to consumer.

One should also be able to see the flink jobs live and running on the job manager UI at http://localhost:8081