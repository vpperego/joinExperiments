package CartesianJoin

import main.startup.{config, spark}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import CartesianJoin.ThetaJoinStorage
import BroadcastJoin.DStreamUtils
import model._

object TpchQ3 {
  val sc = spark.sparkContext;
  sc.getConf.registerKryoClasses(Array(classOf[Customer],classOf[Order],classOf[LineItem],classOf[(Customer,Order)]))

  val ssc = new StreamingContext(sc, Seconds(12))

  val utils = new DStreamUtils

  val customer  = utils.createKafkaStreamTpch(ssc,config("kafkaServer"), Array("customer"), "customer",true)
    .map{line =>
      val fields = line.split('|')
      (Customer(fields(0).toInt), System.currentTimeMillis())
    }

  val order  =   utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("order"), "order",true)
    .map{line =>
      val fields = line.split('|')
      (Order(fields(0).toInt, fields(1).toInt), System.currentTimeMillis())
    }

  val lineItem  = utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("lineitem"), "lineitem",true)
    .map{line =>
      val fields = line.split('|')
      (LineItem(fields(0).toInt,fields(2).toInt), System.currentTimeMillis())
    }

  //  val storage_R = new ThetaJoinStorage[(Int, Long)](sc)
  val customerStorage = new ThetaJoinStorage[(Customer, Long)](sc)
  val orderStorage = new ThetaJoinStorage[(Order, Long)](sc)
  val lineItemStorage = new ThetaJoinStorage[(LineItem, Long)](sc)
  val intermediateStorage = new ThetaJoinStorage[((Customer, Order), Long, Long)](sc)

  val probedCustomer = customerStorage.store(customer)
  val probedOrder = orderStorage.store(order)
  val probedLineItem  = lineItemStorage.store(lineItem)

  val customerJoinPredicate = (pair:((Customer, Long),(Order, Long))) => pair._1._1.custKey == pair._2._1.custKey
  val orderJoinPredicate = (pair:((Customer, Long),(Order, Long))) => pair._1._1.custKey == pair._2._1.custKey
  val lineItemJoinPredicate = (pair:( (((Customer, Order), Long, Long), Long),(LineItem, Long))) => pair._1._1._1._2.orderKey == pair._2._1.orderKey
//  val intermediateJoinPredicate = (pair:( (((Customer, Order), Long, Long), Long),(LineItem, Long))) => pair._1._1._1._2.orderKey == pair._2._1.orderKey


  val customerJoinResult = customerStorage.join(probedOrder,customerJoinPredicate)
  val orderJoinResult = orderStorage.joinAsRight(probedCustomer,orderJoinPredicate)
  val result = customerJoinResult.union(orderJoinResult).map (row => (row, System.currentTimeMillis()))

  intermediateStorage.store(result)
}