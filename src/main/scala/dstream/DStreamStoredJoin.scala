package dstream

import java.util.Properties

import main.startup.{config, spark}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreamStoredJoin {

  var sc = spark.sparkContext
//  sc.getConf.set("spark.streaming.kafka.consumer.cache.enabled","false")
//  sc.getConf.set("spark.streaming.kafka.consumer.cache.maxCapacity","0")
  val ssc = new StreamingContext(sc, Seconds(5))
   val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> config("kafkaServer"),
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val relA = createKafkaStream (Array(config("kafkaTopicA")))
  val relB = createKafkaStream (Array(config("kafkaTopicB")))
//  val relC = createKafkaStream (Array(config("kafkaTopicC")))



//  var streamStreamJoin = join(relA,relB)
//  var storeA  = new Storage(sc)
  var storeB  = new Storage(sc)
   storeB.store(relB)

//  var storeJoinA = storeA.join(relB)
  var storeJoinB = storeB.join(relA)

//  storeA.store(relA)


  var output = storeJoinB
//    .union(storeJoinA)
//    .union(storeJoinB)


////  var storeC: DStream[Int] = new Storage(sc,relA).store()
//
//
//
//  var joinResult: DStream[(Int, Int)] = storeA.transformWith(storeB,
//      (rdd1: RDD[Int], rdd2 : RDD[Int]) => {
//        rdd1.cartesian(rdd2)
//      }
//  )
//
//  output.foreachRDD{ resultRDD =>
//    resultRDD.foreachPartition{ resultPartition =>
//      println("\n\n\nTAMANHO PARTITION =>" + resultPartition.length)
//      if(resultPartition.nonEmpty){
//        val kafkaProps = new Properties()
//        kafkaProps.put("bootstrap.servers", config("kafkaServer"))
//        kafkaProps.put("enable.idempotence", "true") // THIS GUARANTEES EXACTLY ONCE SEMANTIC
//        kafkaProps.put("client.id", "KafkaIntegration Producer");
//        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        val producer = new KafkaProducer[String, String](kafkaProps);
//        resultPartition.foreach{ resultTuple =>
//          var resString = resultTuple._1 + "," + resultTuple._2
//          val message = new ProducerRecord[String, String](config("kafkaTopicOutput"), resString, resString)
//          producer.send(message)
//        }
//        producer.close()
//      }
//    }
//  }

output.print(1000)

  ssc.start
  ssc.awaitTermination

  private def createKafkaStream (topicName: Array[String]): DStream[Int] ={
    KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicName, kafkaParams)
    ).map(_.value.toInt)
  }

  private def join(leftRel: DStream[Int], rightRel: DStream[Int]): DStream[(Int,Int)] = {
    leftRel.transformWith(rightRel,
      (rdd1: RDD[Int], rdd2 : RDD[Int]) => {
        rdd1.cartesian(rdd2)
      }
    )
  }

  /*
      DOESN'T Runs smooth

   */
//  storeA
//    .map(row=> row * 2)
//    .foreachRDD{ resultRDD =>
//      resultRDD.foreachPartition{ resultPartition =>
//        val kafkaProps = new Properties()
//        kafkaProps.put("bootstrap.servers", config("kafkaServer"))
//        kafkaProps.put("enable.idempotence", "true") // THIS GUARANTEES EXACTLY ONCE SEMANTIC
//        kafkaProps.put("client.id", "KafkaIntegration Producer");
//        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        val producer = new KafkaProducer[String, String](kafkaProps);
//        resultPartition.foreach{ resultTuple =>
//          //              var resString = resultTuple._1 + "," + resultTuple._2
//
//          val message = new ProducerRecord[String, String](config("kafkaTopicOutput"), resultTuple.toString, resultTuple.toString)
//          producer.send(message)

//        }
//        producer.close()
//      }
//    }



}
