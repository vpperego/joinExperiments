package dstream

import main.startup.{config, configBroadcast, spark}
import org.apache.log4j.{Level, Logger}
 import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreamStoredJoin {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  var iteratorCounter = 0
  var sc = spark.sparkContext

  val joinConditionA = (pair:((Int, Long),(Int, Long))) => pair._1._1 < pair._2._1 && pair._1._2 < pair._2._2

  val joinConditionB = (pair:((Int, Long),(Int, Long))) => pair._1._1 < pair._2._1 && pair._2._2  < pair._1._2

  val joinConditionIntermediate = (pair:((((Int, Int), Long), Long),(Int, Long))) => pair._1._1._1._2 < pair._2._1 && pair._1._2  < pair._2._2

  val joinConditionC = (pair:((((Int, Int), Long), Long),(Int, Long))) => pair._1._1._1._2 < pair._2._1 && pair._2._2  < pair._1._2

  val ssc = new StreamingContext(sc, Seconds(16))

  var utils = new DStreamUtils

  val relAStream = utils.createKafkaStreamTpch(ssc,config("kafkaServer"),Array(config("kafkaTopicA")),"banana",true).map(row => row.toInt)
  val relBStream = utils.createKafkaStreamTpch (ssc,config("kafkaServer"), Array(config("kafkaTopicB")),"apple",true).map(row => row.toInt)
  val relCStream = utils.createKafkaStreamTpch (ssc,config("kafkaServer"), Array(config("kafkaTopicC")),"grape",true).map(row => row.toInt)
  val storeA = new GenericStorage[Int](sc,"RelA")
  val storeB = new GenericStorage[Int](sc,"RelB")
  val storeC = new GenericStorage[Int](sc,"RelC")
  val intermediateStore = new GenericStorage[((Int, Int), Long)](sc,"Intermediate Result")

  var probedA = storeA.store(relAStream)
  var probedB = storeB.store(relBStream)
  var probedC = storeC.store(relCStream)

  var storeAJoin: DStream[((Int, Int), Long)] = storeA.join(probedB, joinConditionA,750)
  var storeBJoin  = storeB.joinAsRight(probedA, joinConditionB,750)

  val intermediateResult: DStream[((Int, Int), Long)] = storeAJoin
    .union(storeBJoin)

  var probedIntermediate: DStream[(((Int, Int), Long), Long)] = intermediateStore.store(intermediateResult)

  var storeIntermediateJoin  = intermediateStore
    .join(probedC,joinConditionIntermediate,750)


  var storeCJoin: DStream[((((Int, Int), Long), Int), Long)] = storeC.joinAsRight(probedIntermediate, joinConditionC,750)

  var output: DStream[ (Long, Long)] = storeIntermediateJoin.union(storeCJoin)
    .map(resultRow => (resultRow._1._1._2,System.currentTimeMillis())).cache()


  output
     .foreachRDD { resultRDD =>
       var resultSize = resultRDD.count()
       if (resultSize>0) {
         println(s"Result size: $resultSize")
//          println(s"End = ${resultRDD.sortBy(_._3).max()._3 - resultRDD.sortBy(_._2).min()._2}")
//         resultRDD.saveAsTextFile("hdfs:/user/vinicius/tpchQ3Times")
//            resultRDD.saveAsTextFile("file:///tmp/result")
//            var startTime =  resultRDD.keys.min()
//             var endTime   =  resultRDD.values.max()
//           println(s"Time = ${endTime-startTime} ms")

//         val props = new Properties()
//         props.put("bootstrap.servers",configBroadcast.value("kafkaServer") )
//         props.put("client.id", "kafkaProducer")
//         props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//         props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//         val producer = new KafkaProducer[String, String](props)
//         val data = new ProducerRecord[String, String](configBroadcast.value("kafkaTopicOutput"), resultSize.toString())
//         producer.send(data)
//         producer.close()
       }


//       if (resultSize > 0) {
//         println(s"output size: $resultSize")
//         resultRDD.foreachPartition { part =>
//           val props = new Properties()
//           props.put("bootstrap.servers",configBroadcast.value("kafkaServer") )
//           props.put("client.id", "kafkaProducer")
//           props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//           props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//           val producer = new KafkaProducer[String, String](props)
//           part.foreach { row =>
//             val data = new ProducerRecord[String, String](configBroadcast.value("kafkaTopicOutput"), row.toString())
//             producer.send(data)
//
//           }
//           producer.close()
//         }
       }

  println("Waiting for jobs (DStreamStoredJoin)")

  ssc.start
  ssc.awaitTermination

}
