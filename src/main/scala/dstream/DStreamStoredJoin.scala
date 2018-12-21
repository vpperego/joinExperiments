package dstream



import java.util.Properties

import main.startup.{config, spark}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StateSpec, StreamingContext}

object DStreamStoredJoin {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  var sc = spark.sparkContext

  val ssc = new StreamingContext(sc, Seconds(2))


  val relAStream = createKafkaStream (Array(config("kafkaTopicA")),"banana")

  val relBStream = createKafkaStream (Array(config("kafkaTopicB")),"apple")
   val relCStream = createKafkaStream (Array(config("kafkaTopicC")),"grape")

  val storeA = new Storage(sc,ssc,"RelA")
  storeA.store(relAStream)

  val storeB = new Storage(sc,ssc, "RelB")
  storeB.store(relBStream)

  val storeC = new Storage(sc,ssc, "RelC")
  storeC.store(relCStream)



  var storeBJoin: DStream[(Int, Int)] = storeB.join(relAStream,streamLeft = true)
  var storeAJoin: DStream[(Int, Int)] = storeA.join(relBStream,streamLeft = false)


  val intermediateResult: DStream[(Int, Int)] = storeAJoin
    .union(storeBJoin)


  val intermediateStore = new Storage(sc,ssc,"Intermediate Result")
  intermediateStore.storeIntermediateResult(intermediateResult)

  var storeIntermediateJoin: DStream[(Int, Int, Int)] = intermediateStore.intermediateStoreJoin(relCStream,streamLeft = false)


  var storeCJoin: DStream[(Int, Int, Int)] = storeC.joinWithIntermediateResult(intermediateResult,streamLeft = true)

  var output: DStream[(Int, Int, Int)] = storeIntermediateJoin.union(storeCJoin);

  output
      .print(1000)
//    .foreachRDD{ resultRDD =>
//      resultRDD
//        .foreachPartition{ resultPartition =>
//          //      if(resultPartition.nonEmpty){
//          val kafkaProps = new Properties();
//          kafkaProps.put("bootstrap.servers", "dbis-expsrv1:9092" );
//          kafkaProps.put("client.id", "KafkaIntegration Producer");
//          kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//          kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//          val producer = new KafkaProducer[String, String](kafkaProps);
//          resultPartition.foreach{ resultTuple =>
//            var resString = resultTuple.toString;//._1 + "," + resultTuple._2
//          val message = new ProducerRecord[String, String]("storedJoin", resString, resString);
//            producer.send(message)
//          }
//          producer.close()
//          //      }
//        }
//    }

  ssc.start
  ssc.awaitTermination

  private def createKafkaStream (topicName: Array[String],groupName: String): DStream[Int] ={

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> config("kafkaServer"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupName,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicName, kafkaParams)
    )      .map(row => row.value.toInt)
//      .repartition(8)
  }

  private def join(leftRel: DStream[Int], rightRel: DStream[Int]): DStream[(Int,Int)] = {
    leftRel.transformWith(rightRel,
      (rdd1: RDD[Int], rdd2 : RDD[Int]) => {
        rdd1.cartesian(rdd2)
      }
    )
  }
}
