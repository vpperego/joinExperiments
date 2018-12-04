package dstream

import java.util.Properties

import main.startup.{config, spark}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreamCrossProduct {
  var sc = spark.sparkContext
  val ssc = new StreamingContext(sc, Seconds(1))
  val rng = new scala.util.Random

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> config("kafkaServer"),
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )
  val kafkaTopicA = Array(config("kafkaTopicA"))
  val kafkaTopicB = Array(config("kafkaTopicB"))
  val kafkaTopicC = Array(config("kafkaTopicC"))

  private val relA: DStream[Int] = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](kafkaTopicA, kafkaParams)
  ).map(_.value.toInt)

//  val relB = KafkaUtils.createDirectStream[String, String](
//    ssc,
//    PreferConsistent,
//    Subscribe[String, String](kafkaTopicB, kafkaParams)
//  ) .map(_.value.toInt)
//
//  val relC = KafkaUtils.createDirectStream[String, String](
//    ssc,
//    PreferConsistent,
//    Subscribe[String, String](kafkaTopicC, kafkaParams)
//  ) .map(_.value.toInt)



//  var windowedStream1: DStream[Int] = relA.window(Seconds(3),)
//  var windowedStream2: DStream[Int] = relB.window(Minutes(1))
//  var windowedStream3: DStream[Int] = relC.window(Minutes(1))
//
//  var res  = windowedStream1.transformWith(windowedStream2,
//    (rdd1: RDD[Int], rdd2 : RDD[Int]) => {
//      rdd1.cartesian(rdd2)
//    }
//  )
//  var secondJoin  = res.transformWith(windowedStream3,
//    (rdd1: RDD[(Int, Int)], rdd2 : RDD[Int]) => {
//           rdd1.cartesian(rdd2)
//             .map((row: ((Int, Int), Int)) =>(row._1._1,row._1._2,row._2))
//        })
//
//  var finalRes = secondJoin.filter{ case tuple:(Int,Int,Int) =>
//     tuple._1 < tuple._2  && tuple._2 <  tuple._3
//  }
//
//   finalRes
//    .foreachRDD { rdd => {
//      rdd.foreachPartition { partitionOfRecords =>
//
//        val kafkaProps = new Properties()
//        kafkaProps.put("bootstrap.servers", config("kafkaServer"))
//        kafkaProps.put("enable.idempotence", "true") // THIS GUARANTEES EXACTLY ONCE SEMANTIC
//        kafkaProps.put("client.id", "KafkaIntegration Producer");
//        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        //      val producer = new KafkaProducer[String, String](kafkaProps);
//        partitionOfRecords.foreach((record: (Int, Int, Int)) => {
//          var resString = record._1 + "," + record._2 + "," + record._3
//
//                   val message = new ProducerRecord[String, String](config("kafkaTopicOutput"), resString,resString )
//                  producer.send(message)
//        })
//              producer.close()
//      }
//    }
//      rdd.unpersist()
//    }



  relA
    .map(rowA => rowA*2)
    .foreachRDD(rdd=>
  rdd.foreachPartition{partitionOfRdd =>
    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", config("kafkaServer"))
    kafkaProps.put("enable.idempotence", "true") // THIS GUARANTEES EXACTLY ONCE SEMANTIC
    kafkaProps.put("client.id", "KafkaIntegration Producer");
    kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    val producer = new KafkaProducer[String, String](kafkaProps);
    partitionOfRdd.foreach{ myRow =>

               val message = new ProducerRecord[String, String](config("kafkaTopicOutput"), "Nanana",myRow.toString)
              producer.send(message)
    }
          producer.close()

  }
  )

  ssc.start
  ssc.awaitTermination
 }
