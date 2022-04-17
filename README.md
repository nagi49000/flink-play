# flink-play
Simple examples using flink

The demo can be built running (the first build will take a while; lots of docker pulling, docker building and compilation)
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
- a pyflink jobmanager (Apache Flink 1.14.3 running on Scala 2.11, with Python 3.7)
- a pyflink taskmanager (as above)
- zookeeper (for kafka) (3.7.0)
- a single node kafka server (3.1.0)

The kafka producers are:
- a python producer publishing records of the form {'time': '2022-02-07T23:26:17.272739Z'} to kafka topic 'json-time-topic'
- a python producer publishing records of the form {'name': 'james bond'} to kafka topic 'json-small-names-topic'

The kafka consumers are:
- a python consumer subscribing to records of the form {'usecs': 1644276377272739.0} from kafka topic 'json-usecs-topic'. Records are published to docker logs.
- a python consumer subscribing to records of the form {'firstname': 'JAMES', 'totalchars': 69} from kafka topic 'json-big-firstname-topic'. Records are published to docker logs.

The flink jobs are:
- a pyflink job (DataStream API, Python 3.7, Flink 1.14.3) converting iso times to microseconds since unix epoch (submits job to jobmanager)
- a scala flink job (DataStream API, Scala 2.11.12, Flink 1.14.3) converting full names to uppercase first names, and running a stateful accumulation of the number of characters produced (submits job to jobmanager)

On running the demo and observing the docker logs, one should see the services come up, and the records stream through (and get transformed) from producer, to taskmanager and to consumer. The producers have a finite lifespan and rate, which can be adjusted in the associated producer.py files for the producer service.

One should also be able to see the flink jobs live and running on the job manager UI at http://localhost:8081

### References

[Apache Flink website](https://flink.apache.org/)

[Apache Flink Kafka connectors docs](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/datastream/kafka/)

[Apache pyFlink DataStream API docs](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/python/datastream_tutorial/)

[Apache Flink Java/Scala DataStream API docs](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/datastream/overview/)

[Apache Flink Working with State docs](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/state/)

[Apache Flink - mapWithState on KeyedStream](http://www.alternatestack.com/development/apache-flink-mapwithstate-on-keyedstream/)

[Apache Flink mailing lists - including user community](https://flink.apache.org/community.html#mailing-lists)

[Ten Flink Gotchas we wish we had known](https://engineering.contentsquare.com/2021/ten-flink-gotchas/)
