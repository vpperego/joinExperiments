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

  val ssc = new StreamingContext(sc, Seconds(12))
//  ssc.addStreamingListener(new FooListener)

  var utils = new DStreamUtils
  val relAStream = utils.createKafkaStream (ssc,config("kafkaServer"),Array(config("kafkaTopicA")),"banana")

  val relBStream = utils.createKafkaStream (ssc,config("kafkaServer"), Array(config("kafkaTopicB")),"apple")

  val storeA = new NewStorage(sc,ssc,"RelA")
  val storeB = new NewStorage(sc,ssc, "RelB")


  var probedA = storeA.store(relAStream)
  var probedB = storeB.store(relBStream)

  var storeBJoin: DStream[(Int, Int)] = storeB.join(probedA,rightRelStream = false, utils.joinCondition)
  var storeAJoin: DStream[(Int, Int)] = storeA.join(probedB,rightRelStream = true, utils.joinCondition)


  val result: DStream[(Int, Int)] = storeAJoin
    .union(storeBJoin)

  result
    .foreachRDD{ resultRDD =>
      var resultSize = resultRDD.count()
      if (resultSize>0){
        println(s"output size: $resultSize")

      }
    }

  ssc.start
  ssc.awaitTermination
 }
