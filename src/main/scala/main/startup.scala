package main


import dstream.{DStreamCrossProduct, DStreamInput, DStreamStoredJoin}
import org.apache.spark.sql.SparkSession
import structuredStreaming._

object startup extends App {
  var _appName = args(0)
   val spark = SparkSession
    .builder
    .config("spark.streaming.kafka.consumer.cache.enabled","false")
     .appName(_appName)
    .getOrCreate
  val config = spark.sparkContext.textFile(args(1))
    .map(_.split("[\t ]+"))
    .map(arr => arr(0) -> arr(1))
    .collect
    .toMap

  config("class") match {
    case "innerJoin" =>
      spark.conf.set("spark.sql.forceCrossJoin", config("forceCrossJoin"))
      innerJoin.run
    case "kafkaConsumer" => kafkaConsumer.run
    case "kafkaProducer" => kafkaProducer.run
    case "batchJoin" => batchJoin.run
    case "DStreamInput" => DStreamInput.run
    case "DStreamCrossProduct" => DStreamCrossProduct
    case "DStreamStoredJoin" => DStreamStoredJoin
    case "SSStoredJoin" => SSStoredJoin

    case _ => None
  }

}
