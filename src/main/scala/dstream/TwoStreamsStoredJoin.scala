package dstream

import java.util.Properties

import main.startup.{config, configBroadcast, spark}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object TwoStreamsStoredJoin {
//
//  var cartesianJoin = true;
//  Logger.getLogger("org").setLevel(Level.OFF)
//  Logger.getLogger("akka").setLevel(Level.OFF)
//  var iteratorCounter = 0
//  var sc = spark.sparkContext
//
//  val joinConditionA = (pair:((Int, Long),(Int, Long))) => pair._1._1 < pair._2._1 && pair._1._2 < pair._2._2
//
//  val joinConditionB = (pair:((Int, Long),(Int, Long))) => pair._1._1 < pair._2._1 && pair._2._2  < pair._1._2
//
//  val ssc = new StreamingContext(sc, Seconds(1))
////  ssc.addStreamingListener(new FooListener)
//
//  var utils = new DStreamUtils
//  val relAStream = utils.createKafkaStream (ssc,config("kafkaServer"),Array(config("kafkaTopicA")),"banana")
//
//  val relBStream = utils.createKafkaStream (ssc,config("kafkaServer"), Array(config("kafkaTopicB")),"apple")
//
//  val storeA = new GenericStorage[Int](sc,"RelA")
//  val storeB = new GenericStorage[Int](sc,"RelB")
//
//
//  var probedA = storeA.store(relAStream)
//  var probedB = storeB.store(relBStream)
//
//  var storeAJoin: DStream[(Int, Int)] = storeA.join(probedB, joinConditionA)
//  var storeBJoin: DStream[(Int, Int)] = storeB.joinAsRight(probedA, joinConditionB)
//
//
//  val result: DStream[(Int, Int)] = storeAJoin
//    .union(storeBJoin)
//
//  result
//    .foreachRDD { resultRDD =>
//      var resultSize = resultRDD.count()
//
//      if (resultSize > 0) {
//        println(s"Result size: $resultSize")
//        val props = new Properties()
//        props.put("bootstrap.servers", configBroadcast.value("kafkaServer"))
//        props.put("client.id", "kafkaProducer")
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//        val producer = new KafkaProducer[String, String](props)
//        val data = new ProducerRecord[String, String](configBroadcast.value("kafkaTopicOutput"), resultSize.toString())
//        producer.send(data)
//        producer.close()
//      }
//    }
//
//  println("Waiting for jobs (2 streams)")
//   ssc.start
//  ssc.awaitTermination
 }
