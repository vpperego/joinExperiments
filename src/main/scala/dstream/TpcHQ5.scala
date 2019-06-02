package dstream

import main.startup.{config, spark}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

case class Supplier(suppKey: Int, nationKey: Int)

object TpcHQ5 {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  var sc = spark.sparkContext
  sc.getConf.registerKryoClasses(Array(classOf[Customer], classOf[Order], classOf[LineItem], classOf[(Customer, Order)]))

  val ssc = new StreamingContext(sc, Seconds(12))

  var utils = new DStreamUtils

  var customer: DStream[Customer] = utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("customer"), "customer", true)
    .map(_.split('|'))
    .map(fields => Customer(fields(0).toInt))

  var order = utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("order"), "order", true)
    .map(_.split('|'))
    .map(fields => Order(fields(0).toInt, fields(1).toInt))

  var lineItem = utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("lineitem"), "lineitem", true)
    .map(_.split('|'))
    .map(fields => LineItem(fields(0).toInt, fields(2).toInt))

  var supplier: DStream[Supplier] = utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("supplier"), "supplier", true)
    .map(_.split('|'))
    .map(fields => Supplier(fields(0).toInt, fields(3).toInt))

  // STORAGES
  var customerStorage = new GenericStorage[Customer](sc, "customer")
  var orderStorage = new GenericStorage[Order](sc, "order")
  var lineItemStorage = new GenericStorage[LineItem](sc, "lineItem")
  var supplierStorage = new GenericStorage[Supplier](sc, "supplier")
  var coIntermediate = new GenericStorage[((Customer, Order), Long)](sc, "CO-Intermediate")
  var colIntermediate = new GenericStorage[((((Customer, Order), Long), LineItem), Long)](sc, "COL-Intermediate")


  // PROBED STREAMS
  var probedCustomer = customerStorage.store(customer)
  var probedOrder = orderStorage.store(order)
  var probedLineItem = lineItemStorage.store(lineItem)
  var probedSupplier = supplierStorage.store(supplier)


  //JOIN PREDICATES
  val customerJoinPredicate = (pair:((Customer, Long),(Order, Long))) => pair._1._1.custKey == pair._2._1.custKey && pair._1._2 < pair._2._2
  val orderJoinPredicate = (pair:((Customer, Long),(Order, Long))) => pair._1._1.custKey == pair._2._1.custKey && pair._2._2 < pair._1._2
  val lineItemJoinPredicate= (pair:( (((Customer, Order), Long), Long),(LineItem, Long))) =>  pair._1._1._1._2.orderKey ==pair._2._1.orderKey &&  pair._2._2 < pair._1._2
  val coJoinPredicate = (pair:( (((Customer, Order), Long), Long),(LineItem, Long))) =>  pair._1._1._1._2.orderKey ==pair._2._1.orderKey &&  pair._1._2 < pair._2._2
  val supplierJoinPredicate =  (pair:( (((((Customer, Order), Long), LineItem), Long), Long),(Supplier, Long))) =>   pair._1._1._1._2.suppKey == pair._2._1.suppKey &&  pair._2._2 < pair._1._2
  val colJoinPredicate = (pair:( (((((Customer, Order), Long), LineItem), Long), Long),(Supplier, Long))) =>   pair._1._1._1._2.suppKey == pair._2._1.suppKey &&  pair._1._2 < pair._2._2
  //nation predicates
  val nationJoinPredicate =  (pair:( (((((((Customer, Order), Long), LineItem), Long), Supplier), Long), Long),(Nation, Long))) =>   pair._1._1._1._2.nationKey == pair._2._1.nationKey &&  pair._2._2 < pair._1._2
  val colsJoinPredicate = (pair:( (((((((Customer, Order), Long), LineItem), Long), Supplier), Long), Long),(Nation, Long))) =>   pair._1._1._1._2.nationKey == pair._2._1.nationKey &&  pair._1._2 < pair._2._2


  var customerJoinResult: DStream[((Customer, Order), Long, Long)] = customerStorage.join(probedOrder,customerJoinPredicate,orderStorage.storeSize)
  var orderJoinResult: DStream[((Customer, Order), Long, Long)] = orderStorage.joinAsRight(probedCustomer,orderJoinPredicate,customerStorage.storeSize)

  var coResult: DStream[((Customer, Order), Long)] =  customerJoinResult.union(orderJoinResult)
    .map(coRow => ((coRow._1._1, coRow._1._2),coRow._3))
  var coProbed: DStream[(((Customer, Order), Long), Long)] = coIntermediate.store(coResult)

  var intermediateJoinResult: DStream[((((Customer, Order), Long), LineItem), Long, Long)] = coIntermediate
    .join(probedLineItem, coJoinPredicate,lineItemStorage.storeSize)

  var lineItemJoinResult: DStream[((((Customer, Order), Long), LineItem), Long, Long)] =
    lineItemStorage.joinAsRight(coProbed,lineItemJoinPredicate,0L)

  var colResult: DStream[((((Customer, Order), Long), LineItem), Long)] =
    intermediateJoinResult.union(lineItemJoinResult)
      .map(outputRow => (outputRow._1,outputRow._3))

  var colProbed: DStream[(((((Customer, Order), Long), LineItem), Long), Long)] = colIntermediate.store(colResult)

  var colJoinResult: DStream[((((((Customer, Order), Long), LineItem), Long), Supplier), Long, Long)] = colIntermediate.join(probedSupplier,colJoinPredicate, 0L)
  var supJoinresult: DStream[((((((Customer, Order), Long), LineItem), Long), Supplier), Long, Long)] = supplierStorage.joinAsRight(colProbed,supplierJoinPredicate, 0L)

  var result = colJoinResult
    .union(supJoinresult)
  .map(outputRow => (outputRow._1._1._1._1._2, outputRow._1._1._2,outputRow._2))

  result
    .saveAsTextFiles(config("hadoopFileName")+"/" +sc.applicationId+ "/")

  println("Waiting for jobs (TPC-H Q5)")

  ssc.start
  ssc.awaitTerminationOrTimeout(Minutes(config("waitingTime").toInt).milliseconds)

}