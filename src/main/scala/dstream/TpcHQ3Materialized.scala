package dstream


import main.startup.{config, spark}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}



object TpcHQ3Materialized {
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
  var intermediateStorage = new GenericStorage[((Customer,Order), Long)](sc,"Intermediate Storage")


  var probedCustomer = customerStorage.store(customer)
  var probedOrder = orderStorage.store(order)
  var probedLineItem = lineItemStorage.store(lineItem)


  val customerJoinPredicate = (pair:((Customer, Long),(Order, Long))) => pair._1._1.custKey == pair._2._1.custKey && pair._1._2 < pair._2._2
  val orderJoinPredicate = (pair:((Customer, Long),(Order, Long))) => pair._1._1.custKey == pair._2._1.custKey && pair._2._2 < pair._1._2
  val lineItemJoinPredicate = (pair:((((Customer, Order), Long), Long),(LineItem, Long))) => pair._1._1._1._2.orderKey == pair._2._1.orderKey && pair._2._2 < pair._1._2
  val intermediateJoinPredicate = (pair:((((Customer, Order), Long), Long),(LineItem, Long))) => pair._1._1._1._2.orderKey == pair._2._1.orderKey && pair._1._2 < pair._2._2


  var customerJoinResult  = customerStorage.join(probedOrder,customerJoinPredicate)
  var orderJoinResult   = orderStorage.joinAsRight(probedCustomer,orderJoinPredicate)

    var intermediateResult: DStream[((Customer, Order), Long)] =  customerJoinResult.union(orderJoinResult)


  var probedIntermediate: DStream[(((Customer, Order), Long), Long)] = intermediateStorage
                        .store(intermediateResult)

  var intermediateJoinResult  = intermediateStorage
                      .joinFinal(probedLineItem, intermediateJoinPredicate)

  var lineItemJoinResult = lineItemStorage.joinAsRightFinal(probedIntermediate,lineItemJoinPredicate)

  var result: DStream[((((Customer, Order), Long), LineItem), Long, Long)] =  intermediateJoinResult.union(lineItemJoinResult)




  result
      .map(outputRow => (outputRow._1._1._2,outputRow._3)  )
     .saveAsTextFiles(config("hadoopFileName")+"-" +sc.applicationId)

  println("Waiting for jobs (TPC-H Q3 Materialized) ")

  ssc.start
  ssc.awaitTerminationOrTimeout(Minutes(config("waitingTime").toInt).milliseconds)
}
