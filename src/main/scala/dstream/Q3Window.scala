package dstream


import main.startup.{config, spark}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream


object Q3Window {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  var sc = spark.sparkContext;
  sc.getConf.registerKryoClasses(Array(classOf[Customer],classOf[Order],classOf[LineItem],classOf[(Customer,Order)]))

  val ssc = new StreamingContext(sc, Seconds(12))

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

  var probedCustomer = customer.map((_,System.currentTimeMillis()))
  var probedOrder = order.map((_,System.currentTimeMillis()))
  var probedLineItem = lineItem.map((_,System.currentTimeMillis()))

  var customerStorage = probedCustomer.window(Minutes(config("windowTime").toInt))
  var orderStorage = probedOrder.window(Minutes(config("windowTime").toInt))
  var lineItemStorage = probedLineItem.window(Minutes(config("windowTime").toInt))


  var customerJoinResult: DStream[((Customer, Long), (Order, Long))] = customerStorage.transformWith(probedOrder,{ (customerRdd:RDD[(Customer,Long)], orderRdd:RDD[(Order,Long)]) =>
    customerRdd
      .cartesian(orderRdd)
      .filter {case (c: (Customer,Long), o:(Order,Long)) => c._1.custKey == o._1.custKey && c._2 < o._2}
  }
  )

  var orderJoinResult: DStream[((Customer, Long), (Order, Long))] = orderStorage.transformWith(probedCustomer,{ (orderRdd:RDD[(Order,Long)], customerRdd:RDD[(Customer,Long)]) =>
    customerRdd
      .cartesian(orderRdd)
      .filter {case (c: (Customer,Long), o:(Order,Long)) => c._1.custKey == o._1.custKey && o._2 < c._2}
  }
  )

  var probedIntermediate: DStream[(((Customer, Long), (Order, Long)), Long)] = customerJoinResult
    .union(orderJoinResult)
    .map(r => ((r._1,r._2),System.currentTimeMillis()))

  var intermediateStorage: DStream[(((Customer, Long), (Order, Long)), Long)] = probedIntermediate
    .window(Minutes(config("windowTime").toInt))

  var intermediateJoinResult = intermediateStorage.transformWith(probedLineItem,{ (intermediateRdd:RDD[(((Customer, Long), (Order, Long)), Long)], lineItemRdd:RDD[(LineItem,Long)]) =>
    intermediateRdd
      .cartesian(lineItemRdd)
      .filter { case (intermediateRow: (((Customer, Long), (Order, Long)), Long), lineItemRow: (LineItem, Long)) =>
            intermediateRow._1._2._1.orderKey == lineItemRow._1.orderKey && intermediateRow._2 < lineItemRow._2
      }
  }
  )

  var lineItemJoinResult: DStream[((((Customer, Long), (Order, Long)), Long), (LineItem, Long))] = lineItemStorage.transformWith(probedIntermediate,{ (lineItemRdd:RDD[(LineItem,Long)], intermediateRdd:RDD[(((Customer, Long), (Order, Long)), Long)]) =>
        intermediateRdd
          .cartesian(lineItemRdd)
          .filter { case (intermediateRow: (((Customer, Long), (Order, Long)), Long), lineItemRow: (LineItem, Long)) =>
            intermediateRow._1._2._1.orderKey == lineItemRow._1.orderKey && intermediateRow._2 < lineItemRow._2
          }
      }
  )

  intermediateJoinResult
    .union(lineItemJoinResult)
    .map{outputRow =>
      (outputRow._1._1._1._2, outputRow._1._1._2._2,outputRow._1._2,outputRow._2._2)
    }
    .saveAsTextFiles(config("hadoopFileName")+"/" +sc.applicationId+ "/")


  println("Waiting for jobs (TPC-H Q3 Window) ")

  ssc.start()
  ssc.awaitTerminationOrTimeout(Minutes(config("waitingTime").toInt).milliseconds)

}
