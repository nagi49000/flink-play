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
      .keyBy(x => x)  // add a keyBy, so we can use mapWithState, which has to follow the keyBy
                      // https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/state/
                      // http://www.alternatestack.com/development/apache-flink-mapwithstate-on-keyedstream/
      .mapWithState(
        (in: String, count: Option[Int]) => {
          count match {
            case Some(num) => ((in, num + in.length), Some(num + in.length))
            case _ => ((in, in.length), Some(in.length))
          }
        }
      )
      // .map(x => x._1)
      .map(x => (x._1.toUpperCase, x._2))
      .map(x => (x._1.split(" ").head, x._2))
      .map(x => Map("firstname" -> x._1, "totalchars" -> x._2))
      .map(x => Json(DefaultFormats).write(x))

    dataStream.print()  // debug
    dataStream.sinkTo(sink)

    // execute program
    env.execute("Flink Scala small name to big firstname job")
  }
}
