package main

import java.sql.Date

import main.startup.{config, spark}

case class Customer(custKey: Int, mktSegment: String)
case class Order(orderKey: Int,custKey: Int, orderDate: Date, shipPriority: Int)
case class LineItem(orderKey: Int, revenue: Double, shipDate: Date)

object TpcHQ3Batch {
//  Logger.getLogger("org").setLevel(Level.OFF)
//  Logger.getLogger("akka").setLevel(Level.OFF)
  import spark.implicits._

  println("Starting TpcHQ3Batch")
  var sc = spark.sparkContext
  var customer  = spark.read.format("csv")
    .load(config("customerPath")).as[(String)]
    .map{line =>
      var fields = line.split('|')
      Customer(fields(0).toInt,fields(6))
    }



  var order  =   spark.read.format("csv").load(config("ordersPath")).as[(String)]
    .map{line =>
      var fields = line.split('|')
      Order(fields(0).toInt, fields(1).toInt,
        Date.valueOf(fields(4)), fields(7).toInt)
    }

  var lineItem  = spark.read.format("csv").load(config("lineItemPath")).as[(String)]
    .map{line =>
      var fields = line.split('|')
      LineItem(fields(0).toInt, fields(5).toDouble * (1 - fields(6).toDouble),Date.valueOf(fields(10)))
    }


  var p = customer
      .join(order,customer("custKey") === order("custKey"))
      .join(lineItem,order("orderKey")  === lineItem("orderKey"))
      .count()

    println("Result size: " + p)
}
