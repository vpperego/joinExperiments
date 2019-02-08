package dstream

import java.time.LocalDate
import java.util.Properties

import main.startup.{config, configBroadcast, spark}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class Customer(custKey: Int, mktSegment: String)
case class Order(orderKey: Int,custKey: Int, orderDate: LocalDate, shipPriority: Int)
case class LineItem(orderKey: Int, extendedPrice: Double,discount:Double,shipDate: LocalDate)

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
      Order(fields(0).toInt, fields(1).toInt, LocalDate.parse(fields(4)), fields(7).toInt)
    }
    .filter(order => order.orderDate.isBefore(LocalDate.parse("1995-03-13")))

  var lineItem  = utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("lineitem"), "lineitem")
    .map{line =>
      var fields = line.split('|')
      LineItem(fields(0).toInt, fields(5).toDouble,fields(6).toDouble,LocalDate.parse(fields(10)))
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

  var joinResult: DStream[((Int, LocalDate, Int), (Double, Double))] =  intermediateJoinResult.union(lineItemJoinResult).map{
   case ((c:Customer,o: Order),l: LineItem) =>
     ((l.orderKey,o.orderDate,o.shipPriority),(l.extendedPrice,l.discount))
   }
  var result = joinResult.groupByKey.map {
    row =>
      var revenue: Double = row._2.map { case (a1, a2) => a1 * (1 - a2) }.sum
      (row._1._1,revenue,row._1._2,row._1._3)
  }



 result
    .foreachRDD { resultRDD =>
      var resultSize = resultRDD.count()
      if (resultSize > 0) {
        println(s"Result size: ${resultSize}")
//        val props = new Properties()
//        props.put("bootstrap.servers", configBroadcast.value("kafkaServer"))
//        props.put("client.id", "kafkaProducer")
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//        val producer = new KafkaProducer[String, String](props)
//        val data = new ProducerRecord[String, String](configBroadcast.value("kafkaTopicOutput"), resultSize.toString())
//        producer.send(data)
//        producer.close()
        resultRDD.foreachPartition{partition =>
          val props = new Properties()
          props.put("bootstrap.servers", configBroadcast.value("kafkaServer"))
          props.put("client.id", "kafkaProducer")
          props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
          props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
          val producer = new KafkaProducer[String, String](props)
          partition.foreach{row =>
            val data = new ProducerRecord[String, String](configBroadcast.value("kafkaTopicOutput"), row.toString)
            producer.send(data)
          }
          producer.close
        }
      }
    }

  println("Waiting for jobs (TPC-H Q3) ")

  ssc.start
  ssc.awaitTermination
}
