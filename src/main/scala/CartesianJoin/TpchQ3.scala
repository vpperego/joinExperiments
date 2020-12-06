package CartesianJoin

import main.startup.{config, configBroadcast, spark}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import util._

import java.util.Properties

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

  val customerStorage = new ThetaJoinStorage[(Customer, Long)](sc)
  val orderStorage = new ThetaJoinStorage[(Order, Long)](sc)
  val lineItemStorage = new ThetaJoinStorage[(LineItem, Long)](sc)
  val intermediateStorage = new ThetaJoinStorage[(((Customer, Long), (Order, Long)), Long)](sc)

  val probedCustomer = customerStorage.store(customer)
  val probedOrder: DStream[(Order, Long)] = orderStorage.store(order)
  val probedLineItem  = lineItemStorage.store(lineItem)

  val customerOrderPredicate = (customer_row:(Customer, Long), order_row: (Order, Long)) => customer_row._1.custKey == order_row._1.custKey
  val intermediateLineItemPredicate = (intermediate_row: (((Customer, Long), (Order, Long)), Long), line_item_row: (LineItem, Long)) => intermediate_row._1._2._1.orderKey == line_item_row._1.orderKey


  val customerJoinResult = customerStorage.join(probedOrder, customerOrderPredicate)
  val orderJoinResult = orderStorage.joinAsRight(probedCustomer, customerOrderPredicate)
  val intermediateResult: DStream[(((Customer, Long), (Order, Long)), Long)] = customerJoinResult.union(orderJoinResult).map (row => (row, System.currentTimeMillis()))

  val probedIntermediate = intermediateStorage.store(intermediateResult)

  val intermediateStorageJoinResult = intermediateStorage.join(probedLineItem, intermediateLineItemPredicate)
  val lineItemJoinResult = lineItemStorage.joinAsRight(probedIntermediate, intermediateLineItemPredicate)

  intermediateStorageJoinResult
    .union(lineItemJoinResult)
    .foreachRDD{ outputRDD =>
      val props = new Properties()
      props.put("bootstrap.servers", configBroadcast.value("kafkaServer"))
      props.put("client.id", "kafkaProducer")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      val producer = new KafkaProducer[String, String](props)
      outputRDD.foreach{outputRow =>
        val data = new ProducerRecord[String, String](configBroadcast.value("kafkaTopicOutput"), outputRow.toString(), "foo")
        producer.send(data)
      }
      producer.close()
    }
}