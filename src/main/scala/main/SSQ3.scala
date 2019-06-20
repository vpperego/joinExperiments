package main

import org.apache.log4j.{Level, Logger}
import startup.{config, spark}

case class SSCustomer(custKey: Int, cTs: Long)
case class SSOrder(orderKey: Int,custKey: Int, oTs: Long)
case class SSLineItem(orderKey: Int,suppKey: Int, lTs: Long)
object SSQ3 {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  println("STRuctured streaming program")
  import spark.implicits._
println(config("kafkaServer"))

  val customer = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", config("kafkaServer"))
    .option("subscribe", "customer")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .as[(String)]
    .map{line =>
      var fields = line.split('|')
      SSCustomer(fields(0).toInt,System.currentTimeMillis())
    }

  val order  = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", config("kafkaServer"))
    .option("subscribe", "order")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .as[(String)]
    .map{line =>
      var fields = line.split('|')
      SSOrder(fields(0).toInt, fields(1).toInt,System.currentTimeMillis())
    }


  val lineitem  = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", config("kafkaServer"))
    .option("subscribe", "lineitem")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .as[(String)]
    .map{line =>
      var fields = line.split('|')
      SSLineItem(fields(0).toInt,fields(2).toInt,System.currentTimeMillis())
    }

  var foo  = customer
    .join(order,  customer("custKey")  === order("custKey"))
    .join(lineitem,order("orderKey") === lineitem("orderKey"))
    .map(outputRow => (outputRow.getLong(1),outputRow.getLong(4),outputRow.getLong(7),System.currentTimeMillis))
    .select("*")
    .writeStream
    .format("csv")
    .option("checkpointLocation", config("checkpointDir"))
    .option("path", config("hadoopFileName"))
    .start()
    .awaitTermination(config("waitingTime").toInt)


}
