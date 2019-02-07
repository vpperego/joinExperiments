package dstream

import java.time.LocalDate
import java.util.Properties

import main.startup.{config, configBroadcast, spark}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class Customer(custKey: Int, mktSegment: String)
case class Order(orderKey: Int,custKey: Int, orderDate: LocalDate)
case class LineItem(orderKey: Int, shipDate: LocalDate)

object TpcHQ3 {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  var sc = spark.sparkContext;
  val ssc = new StreamingContext(sc, Seconds(6))

  var utils = new DStreamUtils

  var customer  = utils.createKafkaStreamTpch(ssc,config("kafkaServer"), Array("customer"), "customer")
  .map{line =>
     var fields = line.split('|')
    Customer(fields(0).toInt,fields(6))
    }
    .filter{cust => cust.mktSegment == "BUILDING"  }

  var order  =   utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("order"), "order")
    .map{line =>
      var fields = line.split('|')
      Order(fields(0).toInt, fields(1).toInt, LocalDate.parse(fields(4)))
    }
    .filter(order => order.orderDate.isBefore(LocalDate.parse("1995-03-13")))

  var lineItem  = utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("lineitem"), "lineitem")
    .map{line =>
      var fields = line.split('|')
      LineItem(fields(0).toInt, LocalDate.parse(fields(10)))
    }
    .filter(line => line.shipDate.isAfter(LocalDate.parse("1995-03-19")))


  var customerStorage = new GenericStorage[Customer](sc,"customer")
  var orderStorage = new GenericStorage[Order](sc,"order")
  var lineItemStorage = new GenericStorage[LineItem](sc,"lineItem")
  var intermediateStorage = new GenericStorage[(Customer,Order)](sc,"Intermediate Storage")


  var probedCustomer = customerStorage.store(customer)
  var probedOrder = orderStorage.store(order)
  var probedLineItem = lineItemStorage.store(lineItem)


  val custJoinPredicate = (pair:((Customer, Long),(Order, Long))) => pair._1._1.custKey == pair._2._1.custKey && pair._1._2 < pair._2._2
  val orderJoinPredicate = (pair:((Customer, Long),(Order, Long))) => pair._1._1.custKey == pair._2._1.custKey && pair._2._2 < pair._1._2
  val lineItemJoinPredicate = (pair:(((Customer, Order),Long),(LineItem, Long))) => pair._1._1._2.orderKey == pair._2._1.orderKey && pair._2._2 < pair._1._2
  val intermediateJoinPredicate = (pair:(((Customer, Order),Long),(LineItem, Long))) => pair._1._1._2.orderKey == pair._2._1.orderKey && pair._1._2 < pair._2._2


  var customerJoinResult  = customerStorage.join(probedOrder,custJoinPredicate)
  var orderJoinResult   = orderStorage.joinAsRight(probedCustomer,orderJoinPredicate)

  var intermediateResult =  customerJoinResult.union(orderJoinResult)

  var probedIntermediate = intermediateStorage
                        .store(intermediateResult)

  var intermediateJoinResult = intermediateStorage
                      .join(probedLineItem, intermediateJoinPredicate)

  var lineItemJoinResult = lineItemStorage.joinAsRight(probedIntermediate,lineItemJoinPredicate)

  var result =  intermediateJoinResult.union(lineItemJoinResult)

 result
    .foreachRDD { resultRDD =>
      var resultSize = resultRDD.count()
      if (resultSize > 0) {
        println(s"Result size: ${resultSize}")
        val props = new Properties()
        props.put("bootstrap.servers", configBroadcast.value("kafkaServer"))
        props.put("client.id", "kafkaProducer")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](props)
        val data = new ProducerRecord[String, String](configBroadcast.value("kafkaTopicOutput"), resultSize.toString())
        producer.send(data)
        producer.close()
      }
    }

  println("Waiting for jobs (TPC-H Q3) ")

  ssc.start
  ssc.awaitTermination

  //  var lineItemStorage = new GenericStorage[LineItem](sc,"lineItem")
//    customerStorage.store(customer)
// var customer: RDD[Customer] = sc.textFile(config("customer"))
//   .map{line =>
//     var fields = line.split('|')
//     Customer(fields(0).toInt,fields(6))
//   }.filter{cust => cust.mktSegment.equals(configBroadcast("mktSegment")) }
//
//  var order: RDD[Order] = sc.textFile(config("order"))
//    .map{line =>
//      var fields = line.split('|')
//      Order(fields(0).toInt, fields(1).toInt, LocalDate.parse(fields(4)))
//    }.filter(order => order.orderDate.isBefore(orderDateFilter.value))
//
//  var lineItem: RDD[LineItem] = sc.textFile(config("lineItem"))
//    .map{line =>
//      var fields = line.split('|')
//      LineItem(fields(0).toInt, LocalDate.parse(fields(10)))
//    }.filter(line => line.shipDate.isAfter(lineItemFilter.value))
}
