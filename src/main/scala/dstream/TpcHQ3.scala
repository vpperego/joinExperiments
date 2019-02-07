package dstream

import java.time.LocalDate
import java.util.Properties

import main.startup.{config, configBroadcast, spark}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

case class Customer(custKey: Int, mktSegment: String)
case class Order(orderKey: Int,custKey: Int, orderDate: LocalDate)
case class LineItem(orderKey: Int, shipDate: LocalDate)

object TpcHQ3 {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)



    var sc = spark.sparkContext;
  val ssc = new StreamingContext(sc, Seconds(2))

  var orderDateFilter = sc.broadcast(LocalDate.parse("1995-03-13"))
//  var lineItemFilter = sc.broadcast(LocalDate.parse("1995-03-19"))

  var customerFilter: Broadcast[String] = sc.broadcast("BUILDING")


  var utils = new DStreamUtils

  var customer  = utils.createKafkaStreamTpch(ssc,config("kafkaServer"), Array("customer"), "customer")
  .map{line =>
//      println(line)
         var fields = line.split('|')

    Customer(fields(0).toInt,fields(6))

       }
    .filter{cust => cust.mktSegment == "BUILDING"  }

  var order  =   utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("order"), "order")
          .map{line =>
//            println("New order")
            var fields = line.split('|')
            Order(fields(0).toInt, fields(1).toInt, LocalDate.parse(fields(4)))
          }.filter(order => order.orderDate.isBefore(LocalDate.parse("1995-03-13")))
//
//  var lineItem  = utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array(config("lineItem")), "lineItem")
//    .map{line =>
//      var fields = line.split('|')
//      LineItem(fields(0).toInt, LocalDate.parse(fields(10)))
//    }.filter(line => line.shipDate.isAfter(lineItemFilter.value))


  var customerStorage = new GenericStorage[Customer](sc,"customer")
  var orderStorage = new GenericStorage[Order](sc,"order")

  var probedCustomer = customerStorage.store(customer)
  var probedOrder = orderStorage.store(order)


  val custJoin = (pair:((Customer, Long),(Order, Long))) => pair._1._1.custKey == pair._2._1.custKey && pair._1._2 < pair._2._2
  val orderJoin = (pair:((Customer, Long),(Order, Long))) => pair._1._1.custKey == pair._2._1.custKey && pair._2._2 < pair._1._2


  var a: DStream[(Customer, Order)] = customerStorage.join(probedOrder,custJoin)
  var b: DStream[(Customer, Order)] = orderStorage.joinAsRight(probedCustomer,orderJoin);

  var result =  a.union(b)

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
