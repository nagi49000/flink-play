package org.flinkplay

import org.apache.flink.streaming.api.scala._
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.formats.json.JsonNodeDeserializationSchema
import org.json4s.native.Json
import org.json4s.DefaultFormats


object FlinkNameJob {
  def main(args: Array[String]) {
    println("entering FlinkNameJob.main")
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = KafkaSource.builder()
      .setBootstrapServers("kafka-flink-play:9092")
      .setTopics("json-small-names-topic")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new JsonNodeDeserializationSchema())
      .build()

    val sink = KafkaSink.builder()
      .setBootstrapServers("kafka-flink-play:9092")
      .setRecordSerializer(
        KafkaRecordSerializationSchema.builder()
          .setTopic("json-big-firstname-topic")
          .setValueSerializationSchema(new SimpleStringSchema())
          .build()
      )
      .build();

    val dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
      .map(x => x.get("name").asText)
      .map(x => x.toUpperCase)
      .map(x => x.split(" ").head)
      .keyBy(x => None)  // add a keyBy, so we can use mapWithState, which has to follow the keyBy
                         // here the keyBy is a single value; the keyBy could be a summary of "x", in which case there will be a ValueStore for each keyBy
                         // https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/state/
                         // http://www.alternatestack.com/development/apache-flink-mapwithstate-on-keyedstream/
      .mapWithState(
        (in: String, count: Option[Int]) => {
          // input of this mapWithState block is a 2-tuple of (string, ValueStore), and output is ((string, int), ValueStore)
          // only the first element of the 2-tuples are pulled in from the previous stage, and sent to the next stage,
          // the ValueStore, "count", which is of class Option (hence the Option/Some/None idiom), will be stored by Flink, and has scope ONLY within mapWithState
          // BEWARE, mapWithState does not have anything to put in a TTL on the state, so the state will live forever (scary if you are generating new unique keyBys).
          //   - https://engineering.contentsquare.com/2021/ten-flink-gotchas/
          count match {
            case Some(num) => ((in, num + in.length), Some(num + in.length))
            case None => ((in, in.length), Some(in.length))
          }
        }
      )
      .map(x => Map("firstname" -> x._1, "totalchars" -> x._2))
      .map(x => Json(DefaultFormats).write(x))

    dataStream.print()  // debug
    dataStream.sinkTo(sink)

    // execute program
    env.execute("Flink Scala small name to big firstname job")
  }
}
