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

    val dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
      .map(x => x.get("name").asText())
      .map(x => x.toUpperCase())
      .map(x => x.split(" ").head)
      .map(x => Map("firstname" -> x))
      .map(x => Json(DefaultFormats).write(x))

    dataStream.print()  // debug

    val sink = KafkaSink.builder()
      .setBootstrapServers("kafka-flink-play:9092")
      .setRecordSerializer(
        KafkaRecordSerializationSchema.builder()
          .setTopic("json-big-firstname-topic")
          .setValueSerializationSchema(new SimpleStringSchema())
          .build()
      )
      .build();

    dataStream.sinkTo(sink)

    // execute program
    env.execute("Flink Scala small name to big firstname job")
  }
}
