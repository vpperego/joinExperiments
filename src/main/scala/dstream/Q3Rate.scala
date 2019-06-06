package dstream

import main.startup.{config, spark}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object Q3Rate {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  var sc = spark.sparkContext;
  sc.getConf.registerKryoClasses(Array(classOf[Customer],classOf[Order],classOf[LineItem],classOf[(Customer,Order)]))

  val ssc = new StreamingContext(sc, Seconds(config("windowSize").toInt))

  var utils = new DStreamUtils

  var customer  = utils.createKafkaStreamTpch(ssc,config("kafkaServer"), Array("customer"), "customer",true)
    .map{line =>
      var fields = line.split('|')
      Customer(fields(0).toInt)
    }

  var order  =   utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("order"), "order",true)
    .map{line =>
      var fields = line.split('|')
      Order(fields(0).toInt, fields(1).toInt)
    }


  var lineItem  = utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("lineitem"), "lineitem",true)
    .map{line =>
      var fields = line.split('|')
      LineItem(fields(0).toInt,fields(2).toInt)
    }


  var customerStorage = new GenericStorage[Customer](sc,"customer")
  var orderStorage = new GenericStorage[Order](sc,"order")
  var lineItemStorage = new GenericStorage[LineItem](sc,"lineItem")
  var intermediateStorage = new GenericStorage[((Customer, Order), Long, Long)](sc,"Intermediate Storage")


  var probedCustomer = customerStorage.store(customer)
  var probedOrder = orderStorage.store(order)
  var probedLineItem: DStream[(LineItem, Long)] = lineItemStorage.store(lineItem)


  val customerJoinPredicate = (pair:((Customer, Long),(Order, Long))) => pair._1._1.custKey == pair._2._1.custKey && pair._1._2 < pair._2._2
  val orderJoinPredicate = (pair:((Customer, Long),(Order, Long))) => pair._1._1.custKey == pair._2._1.custKey && pair._2._2 < pair._1._2
  val lineItemJoinPredicate = (pair:( (((Customer, Order), Long, Long), Long),(LineItem, Long))) => pair._1._1._1._2.orderKey == pair._2._1.orderKey && pair._2._2 < pair._1._2
  val intermediateJoinPredicate = (pair:( (((Customer, Order), Long, Long), Long),(LineItem, Long))) => pair._1._1._1._2.orderKey == pair._2._1.orderKey && pair._1._2 < pair._2._2


  var customerJoinResult: DStream[((Customer, Order), Long, Long)] = customerStorage.join(probedOrder,customerJoinPredicate,orderStorage.storeSize)
  var orderJoinResult: DStream[((Customer, Order), Long, Long)] = orderStorage.joinAsRight(probedCustomer,orderJoinPredicate,customerStorage.storeSize)

  var intermediateResult: DStream[((Customer, Order), Long, Long)] =  customerJoinResult.union(orderJoinResult)

  var probedIntermediate  = intermediateStorage
    .store(intermediateResult, aproximateSize =customerStorage.storeSize+orderStorage.storeSize)

  var intermediateJoinResult: DStream[((((Customer, Order), Long, Long), LineItem), Long, Long)] = intermediateStorage
    .join(probedLineItem, intermediateJoinPredicate,lineItemStorage.storeSize)

  var lineItemJoinResult =
    lineItemStorage.joinAsRight(probedIntermediate,lineItemJoinPredicate,intermediateStorage.storeSize)

  var result =  intermediateJoinResult.union(lineItemJoinResult)
    //    .map(resultRow => (resultRow._1._1._2,System.currentTimeMillis()))
    .map(resultRow => (resultRow._1._1._3,resultRow._3))

  result
    //
    .saveAsTextFiles(config("hadoopFileName")+"/" +sc.applicationId+ "/")

  println("Waiting for jobs (TPC-H Q3) - Rate")

  ssc.start
  ssc.awaitTerminationOrTimeout(Minutes(config("waitingTime").toInt).milliseconds)
}