package dstream


import main.startup.{config, spark}

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream


object WindowJoin {

  val sc = spark.sparkContext
  val ssc = new StreamingContext(sc, Seconds(10))

  var utils = new DStreamUtils
  var streamA = utils.createKafkaStreamTpch(ssc,config("kafkaServer"), Array("kafkaTopicA"), "customer",true).map(_.toInt)
  var streamB = utils.createKafkaStreamTpch(ssc,config("kafkaServer"), Array("kafkaTopicB"), "order",true).map(_.toInt)
//  var streamC = utils.createKafkaStreamTpch(ssc,config("kafkaServer"), Array("relC"), "lineitem",true).map(_.toInt)

  var storeA  = streamA.window(Minutes(10))
  val storeB  = streamB.window(Minutes(10))
//  val storeC  = streamC.window(Minutes(10))

 var joinA: DStream[(Int, Int)] = storeA.transformWith(streamB,{ (stoA:RDD[Int], strB:RDD[Int]) =>
   stoA.cartesian(strB);
 }
 )

  var joinB: DStream[(Int, Int)] = storeB.transformWith(streamA,{ (stoB:RDD[Int], strA:RDD[Int]) =>
    stoB.cartesian(strA);
  }
  )

  joinA
    .union(joinB)
    .print(10000000)

ssc.start()
    ssc.awaitTermination()

}
