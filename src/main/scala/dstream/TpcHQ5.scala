package dstream

import java.sql.Date

import main.startup.{config, spark}
import org.apache.log4j.{Level, Logger}
 import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class Customer(custKey: Int)
case class Order(orderKey: Int,custKey: Int)
case class LineItem(orderKey: Int,suppKey: Int)
case class Supplier(suppKey: Int, nationKey: Int)
case class Nation(nationKey: Int, regionKey: Int)
case class Region(regionKey: Int)



object TpcHQ5 {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  var sc = spark.sparkContext
  sc.getConf.registerKryoClasses(Array(classOf[Customer],classOf[Order],classOf[LineItem],classOf[(Customer,Order)]))

  val ssc = new StreamingContext(sc, Seconds(12))

  var utils = new DStreamUtils

  var customer: DStream[Customer] = utils.createKafkaStreamTpch(ssc,config("kafkaServer"), Array("customer"), "customer",true)
    .map(_.split('|'))
    .map(fields => Customer(fields(0).toInt))

 var order  =   utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("order"), "order",true)
   .map(_.split('|'))
   .map(fields =>  Order(fields(0).toInt, fields(1).toInt))
  var lineItem  = utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("lineitem"), "lineitem",true)
    .map(_.split('|'))
    .map(fields => LineItem(fields(0).toInt,fields(2).toInt))


  var supplier: DStream[Supplier] = utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("lineitem"), "supplier",true)
    .map(_.split('|'))
    .map(fields => Supplier(fields(0).toInt,  fields(3).toInt))

  var nation  = utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("lineitem"), "nation",true)
    .map(_.split('|'))
    .map(fields => Nation(fields(0).toInt,fields(2).toInt))



  var region  = utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("lineitem"), "region",true)
    .map(_.split('|'))
    .map(fields => Region(fields(0).toInt))



  var customerStorage = new GenericStorage[Customer](sc,"customer")
  var orderStorage = new GenericStorage[Order](sc,"order")
  var lineItemStorage = new GenericStorage[LineItem](sc,"lineItem")
  var supplierStorage = new GenericStorage[Supplier](sc,"supplier")
//  var intermediateStorage = new GenericStorage[((Customer,Order),Long)](sc,"Intermediate Result")
//
//
//
//
//  var probedCustomer = customerStorage.store(customer)
//  var probedOrder = orderStorage.store(order)
//  var probedLineItem = lineItemStorage.store(lineItem)
//  var probedSupplier = supplierStorage.store(supplier)
//
//  val customerJoinPredicate = (pair:((Customer, Long),(Order, Long))) => pair._1._1.custKey == pair._2._1.custKey && pair._1._2 < pair._2._2
//  val orderJoinPredicate = (pair:((Customer, Long),(Order, Long))) => pair._1._1.custKey == pair._2._1.custKey && pair._2._2 < pair._1._2
//  val lineItemJoinPredicate = (pair:((LineItem, Long),(Supplier, Long))) => pair._1._1.suppKey == pair._2._1.suppKey && pair._1._2 < pair._2._2
//  val supplierJoinPredicate = (pair:((LineItem, Long),(Supplier, Long))) => pair._1._1.suppKey == pair._2._1.suppKey && pair._2._2 < pair._1._2
//  val finalJoinPredicate = (pair:(((Customer, Order), Long),((LineItem, Supplier), Long))) =>  pair._1._1._2.orderKey == pair._2._1._1.orderKey && pair._2._2 < pair._1._2
//
//
//  var customerJoinResult  = customerStorage.join(probedOrder,customerJoinPredicate,orderStorage.storeSize)
//  var orderJoinResult   = orderStorage.joinAsRight(probedCustomer,orderJoinPredicate,customerStorage.storeSize)
//
//  var intermediateResult: DStream[((Customer, Order), Long)] =  customerJoinResult.union(orderJoinResult)
//  intermediateStorage.store(intermediateResult)
//
//  var supPart: DStream[((LineItem, Supplier), Long)] = supplierStorage.joinAsRight(probedLineItem,supplierJoinPredicate,lineItemStorage.storeSize)
//  var lineItemPart: DStream[((LineItem, Supplier), Long)] = lineItemStorage.join(probedSupplier,lineItemJoinPredicate,lineItemStorage.storeSize)
//
//  var outputRightPart: DStream[((LineItem, Supplier), Long)] = supPart.union(lineItemPart)
//
//  intermediateStorage
//    .join(outputRightPart,finalJoinPredicate,0L)

}
