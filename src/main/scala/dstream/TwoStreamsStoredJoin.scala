package dstream

import dstream.DStreamStoredJoin.ssc
import main.startup.{config, spark}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object TwoStreamsStoredJoin {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  var iteratorCounter = 0
  var sc = spark.sparkContext

  val ssc = new StreamingContext(sc, Seconds(2))

  var utils = new DStreamUtils
  val relAStream = utils.createKafkaStream (ssc,config("kafkaServer"),Array(config("kafkaTopicA")),"banana")

  val relBStream = utils.createKafkaStream (ssc,config("kafkaServer"), Array(config("kafkaTopicB")),"apple")

  val storeA = new Storage(sc,ssc,"RelA")
  val storeB = new Storage(sc,ssc, "RelB")


  var probedA = storeA.store(relAStream)
  var probedB = storeB.store(relBStream)

  var storeBJoin: DStream[(Int, Int)] = storeB.join(probedA,streamLeft = true, utils.joinCondition)
  var storeAJoin: DStream[(Int, Int)] = storeA.join(probedB,streamLeft = false, utils.joinCondition)


  val intermediateResult: DStream[(Int, Int)] = storeAJoin
    .union(storeBJoin)

  intermediateResult
    .foreachRDD{ resultRDD =>
      var resultSize = resultRDD.count()
      if (resultSize>0){
        println(s"output size: $resultSize")

      }
    }

  ssc.start
  ssc.awaitTermination
 }
