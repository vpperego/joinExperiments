package main


import dstream.{DStreamStoredJoin, TwoStreamsStoredJoin}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import structuredStreaming._


object startup extends App {
  var _appName = args(0)
   val spark = SparkSession
    .builder
    .config("spark.streaming.kafka.consumer.cache.enabled","false")
     .config("spark.streaming.backpressure.enabled","true")
//    .config("spark.metrics.conf.*.sink.graphite.class", "org.apache.spark.metrics.sink.GraphiteSink")
//  .config("spark.metrics.conf.*.sink.graphite.host", "192.168.2.9")
//     .config("spark.metrics.conf.*.sink.graphite.port", "2003")
    .config("spark.network.timeout","500s")

    .appName(_appName)
    .getOrCreate

  val config = spark.sparkContext.textFile(args(1))
    .map(_.split("[\t ]+"))
    .map(arr => arr(0) -> arr(1))
    .collect
    .toMap

  val configBroadcast: Broadcast[Map[String, String]] = spark.sparkContext.broadcast(config)

  config("class") match {
    case "innerJoin" =>
      spark.conf.set("spark.sql.forceCrossJoin", config("forceCrossJoin"))
      innerJoin.run
    case "kafkaConsumer" => kafkaConsumer.run
    case "batchJoin" => batchJoin.run
    case "DStreamStoredJoin" => DStreamStoredJoin
    case "TwoStreamsStoredJoin" => TwoStreamsStoredJoin
    case "SSStoredJoin" => SSStoredJoin
    case _ => None
  }

 }
