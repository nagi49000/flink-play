package org.flinkplay

import org.apache.flink.streaming.api.scala._
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.formats.json.JsonNodeDeserializationSchema


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
      // .map(x => JsonNodeFactory.instance.objectNode().put("firstname", x))

    dataStream.print()

    val asNode = dataStream.map(x => Map("firstname" -> x))
    asNode.print()

    /**
     * Here, you can start creating your execution plan for Flink.
     *
     * Start with getting some data from the environment, like
     * env.readTextFile(textPath);
     *
     * then, transform the resulting DataSet[String] using operations
     * like:
     *   .filter()
     *   .flatMap()
     *   .join()
     *   .group()
     *
     * and many more.
     * Have a look at the programming guide:
     *
     * http://flink.apache.org/docs/latest/programming_guide.html
     *
     * and the examples
     *
     * http://flink.apache.org/docs/latest/examples.html
     *
     */


    // execute program
    env.execute("Flink Scala small name to big firstname job")
  }
}
