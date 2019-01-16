package main

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import startup.{config, spark}

object batchJoin {
  def run: Unit = {
    var relA = spark.sparkContext.textFile(config("relASource")).map(_.toInt)
    var relB = spark.sparkContext.textFile(config("relBSource")).map(_.toInt)
    var relC = spark.sparkContext.textFile(config("relCSource")).map(_.toInt)

    var total = relA.cartesian(relB)
      .filter{ case (a,b) => a < b}
      .cartesian(relC)
      .filter{ case ((a,b),c) => b< c}
      .count

    val props = new Properties()
    props.put("bootstrap.servers", config("kafkaServer"))
    props.put("client.id", "kafkaProducer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val data = new ProducerRecord[String, String](config("kafkaTopic"),
      config("kafkaServer"), "Batch Join count: " + total)

    producer.send(data)
  }
}
