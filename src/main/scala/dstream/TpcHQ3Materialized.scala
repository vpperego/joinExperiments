package dstream

import java.sql.Date

import main.startup.{config, spark}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}



object TpcHQ3Materialized {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  var sc = spark.sparkContext;
  sc.getConf.registerKryoClasses(Array(classOf[Customer],classOf[Order],classOf[LineItem],classOf[(Customer,Order)]))

  val ssc = new StreamingContext(sc, Seconds(12))

  var utils = new DStreamUtils

  var customer  = utils.createKafkaStreamTpch(ssc,config("kafkaServer"), Array("customer"), "customer",true)
    .map(_.split('|'))
    .map(fields => Customer(fields(0).toInt))


  var order  =   utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("order"), "order",true)
    .map(_.split('|'))
    .map(fields =>  Order(fields(0).toInt, fields(1).toInt))


  var lineItem  = utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("lineitem"), "lineitem",true)
    .map(_.split('|'))
    .map(fields => LineItem(fields(0).toInt))


  var customerStorage = new GenericStorage[Customer](sc,"customer")
  var orderStorage = new GenericStorage[Order](sc,"order")
  var lineItemStorage = new GenericStorage[LineItem](sc,"lineItem")
  var intermediateStorage = new GenericStorage[(Customer,Order)](sc,"Intermediate Storage")


  var probedCustomer = customerStorage.store(customer)
  var probedOrder = orderStorage.store(order)
  var probedLineItem = lineItemStorage.store(lineItem)


  val customerJoinPredicate = (pair:((Customer, Long),(Order, Long))) => pair._1._1.custKey == pair._2._1.custKey && pair._1._2 < pair._2._2
  val orderJoinPredicate = (pair:((Customer, Long),(Order, Long))) => pair._1._1.custKey == pair._2._1.custKey && pair._2._2 < pair._1._2
  val lineItemJoinPredicate = (pair:(((Customer, Order), Long),(LineItem, Long))) => pair._1._1._2.orderKey == pair._2._1.orderKey && pair._2._2 < pair._1._2
  val intermediateJoinPredicate = (pair:(((Customer, Order), Long),(LineItem, Long))) => pair._1._1._2.orderKey == pair._2._1.orderKey && pair._1._2 < pair._2._2


  var customerJoinResult  = customerStorage.join(probedOrder,customerJoinPredicate)
  var orderJoinResult   = orderStorage.joinAsRight(probedCustomer,orderJoinPredicate)

  var intermediateResult: DStream[((Customer, Order), Long)] =  customerJoinResult.union(orderJoinResult)


  var probedIntermediate: DStream[((Customer, Order), Long)] = intermediateStorage
                        .storeWithTimestamp(intermediateResult)

  var intermediateJoinResult  = intermediateStorage
                      .joinFinal(probedLineItem, intermediateJoinPredicate)

  var lineItemJoinResult = lineItemStorage.joinAsRightFinal(probedIntermediate,lineItemJoinPredicate)

  var result: DStream[(((Customer, Order), LineItem), Long, Long)] =  intermediateJoinResult.union(lineItemJoinResult)



  result
    .foreachRDD { resultRDD =>
      var resultSize = resultRDD.count()
      if (resultSize > 0) {
        println(s"Result size: ${resultSize}")
//        var startTime =  resultRDD.keys.min()
//        var endTime   =  resultRDD.values.max()
//
//        resultRDD.saveAsTextFile("hdfs:/user/vinicius/tpchQ3Times")
//        var time =endTime-startTime
//        val msg = resultSize.toString +"," + time.toString +","+(resultSize/time)
//
//        val props = new Properties()
//          props.put("bootstrap.servers", "localhost:9092")
//          props.put("client.id", "kafkaProducer")
//          props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//          props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//          val producer = new KafkaProducer[String, String](props)
//
//        val data = new ProducerRecord[String, String]("storedJoin", msg)
//            producer.send(data)
//          producer.close
      }
    }

  println("Waiting for jobs (TPC-H Q3) ")

  ssc.start
  ssc.awaitTerminationOrTimeout(Minutes(5).milliseconds)
}
