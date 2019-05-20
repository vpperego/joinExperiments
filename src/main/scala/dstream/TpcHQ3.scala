package dstream

import java.sql.Date

import dstream.TpcHQ3Materialized.ssc
import main.startup.{config, spark}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}



object TpcHQ3 {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  var sc = spark.sparkContext;
  sc.getConf.registerKryoClasses(Array(classOf[Customer],classOf[Order],classOf[LineItem],classOf[(Customer,Order)]))

  val ssc = new StreamingContext(sc, Seconds(4))

  var utils = new DStreamUtils

  var customer  = utils.createKafkaStreamTpch(ssc,config("kafkaServer"), Array("customer"), "customer",true)
    .map(_.split('|'))
    .map(fields => Customer(fields(0).toInt))


  var order  =   utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("order"), "order",true)
    .map(_.split('|'))
    .map(fields =>  Order(fields(0).toInt, fields(1).toInt))


  var lineItem  = utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("lineitem"), "lineitem",true)
    .map(_.split('|'))
    .map(fields => LineItem(fields(0).toInt,fields(2).toInt))


  var customerStorage = new GenericStorage[Customer](sc,"customer")
  var orderStorage = new GenericStorage[Order](sc,"order")
  var lineItemStorage = new GenericStorage[LineItem](sc,"lineItem")


  var probedCustomer = customerStorage.store(customer)
  var probedOrder = orderStorage.store(order)
  var probedLineItem = lineItemStorage.store(lineItem)



  val customerJoinPredicate = (pair:((Customer, Long),(Order, Long))) => pair._1._1.custKey == pair._2._1.custKey && pair._1._2 < pair._2._2
  val orderJoinPredicate = (pair:((Customer, Long),(Order, Long))) => pair._1._1.custKey == pair._2._1.custKey && pair._2._2 < pair._1._2
  val lineItemJoinPredicate = (pair:(((Customer, Order), Long),(LineItem, Long))) => pair._1._1._2.orderKey == pair._2._1.orderKey && pair._2._2 < pair._1._2
  val intermediateJoinPredicate = (pair:((((Customer,Order),Long),Long),(LineItem, Long))) => pair._1._1._1._2.orderKey == pair._2._1.orderKey && pair._1._2 < pair._2._2

  val itemOrderJoinPredicate = (pair:((Order, Long),(LineItem, Long))) => pair._1._1.orderKey == pair._2._1.orderKey && pair._2._2 < pair._1._2
  val customerOrderJoinPredicate = (pair:((Customer, Long),((Order, LineItem), Long))) => pair._1._1.custKey == pair._2._1._1.custKey && pair._1._2 < pair._1._2


  // c⋈O⋈L
  var customerOrderJoin: DStream[((Customer, Order), Long)] = orderStorage.joinAsRight(probedCustomer,orderJoinPredicate)
  var output1=  lineItemStorage.joinAsRightFinal(customerOrderJoin,lineItemJoinPredicate)

  // o⋈C⋈L
  var orderCustomerJoin  = customerStorage.join(probedOrder,customerJoinPredicate)
  var output2 = lineItemStorage.joinAsRightFinal(orderCustomerJoin,lineItemJoinPredicate)

  // l⋈O⋈C
  var lineItemOrderJoin: DStream[((Order, LineItem), Long)] = orderStorage.join(probedLineItem,itemOrderJoinPredicate)
  var  output3
  = customerStorage.joinFinal(lineItemOrderJoin,customerOrderJoinPredicate)
      .map(resultRow => (((resultRow._1._1,resultRow._1._2._1),resultRow._1._2._2),resultRow._2,resultRow._3))



  output1
      .union(output2)
      .union(output3)
      .foreachRDD { resultRDD =>
//      var resultSize = resultRDD.count()
//    if (resultSize > 0) {
      println(s"Result size: ${resultRDD.count()}")
//    }
   }
  println("Waiting for jobs (TPC-H Q3) ")
  ssc.start
  ssc.awaitTerminationOrTimeout(Minutes(config("waitingTime").toInt).milliseconds)
}
