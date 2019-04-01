package dstream


import main.startup.{config, spark}

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream


object WindowJoin {

  val sc = spark.sparkContext
  val ssc = new StreamingContext(sc, Seconds(10))

  var utils = new DStreamUtils
  var streamA = utils.createKafkaStreamTpch(ssc,config("kafkaServer"), Array(config("kafkaTopicA")), "customer",true).map(_.toInt)
  var streamB = utils.createKafkaStreamTpch(ssc,config("kafkaServer"), Array(config("kafkaTopicB")), "order",true).map(_.toInt)
  var streamC = utils.createKafkaStreamTpch(ssc,config("kafkaServer"), Array(config("kafkaTopicC")), "lineitem",true).map(_.toInt)

  var storeA  = streamA.window(Minutes(10))
  val storeB  = streamB.window(Minutes(10))
  val storeC  = streamC.window(Minutes(10))

  var joinA: DStream[(Int, Int)] = storeA.transformWith(streamB,{ (stoA:RDD[Int], strB:RDD[Int]) =>
    stoA.cartesian(strB);
  }
  )

  var joinB: DStream[(Int, Int)] = storeB.transformWith(streamA,{ (stoB:RDD[Int], strA:RDD[Int]) =>
    stoB.cartesian(strA);
  }
  )

  var intermediateWindowJoin = joinA
    .union(joinB)
    .window(Minutes(10))

  var output1 = storeC.transformWith(intermediateWindowJoin,{ (stoC:RDD[Int], intSt:RDD[(Int,Int)]) =>
    intSt.cartesian(stoC);
  }
  )

  var output2 = intermediateWindowJoin.transformWith(storeC,{ (intSt:RDD[(Int,Int)],stoC:RDD[Int] ) =>
    intSt.cartesian(stoC);
  }
  )


  output1.union(output2)
    .print(10000000)

  ssc.start()
  ssc.awaitTermination()

}
