package dstream

import java.util.Properties

import main.startup.{config, configBroadcast, spark}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object TwoStreamsStoredJoin {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  var iteratorCounter = 0
  var sc = spark.sparkContext

  val ssc = new StreamingContext(sc, Seconds(10))
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

       if (resultSize>0) {
        val props = new Properties()
        props.put("bootstrap.servers",configBroadcast.value("kafkaServer") )
        props.put("client.id", "kafkaProducer")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](props)
        val data = new ProducerRecord[String, String](configBroadcast.value("kafkaTopicOutput"), resultSize.toString())
        producer.send(data)
        producer.close()
      }



//      if (resultSize>0){
//        println(s"output size: $resultSize")
//        resultRDD.foreachPartition{part =>
//          val props = new Properties()
//          props.put("bootstrap.servers",configBroadcast.value("kafkaServer") )
//          props.put("client.id", "kafkaProducer")
//          props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//          props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//          val producer = new KafkaProducer[String, String](props)
//          part.foreach{row =>
//            val data = new ProducerRecord[String, String](configBroadcast.value("kafkaTopicOutput"), row.toString())
//            producer.send(data)
//
//          }
//          producer.close()
//        }
//      }
    }

  println("Waiting for jobs (2 streams)")
   ssc.start
  ssc.awaitTermination
 }
