package main

import java.sql.Date

import main.startup.spark

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
    .load("file:///home/vinicius/IdeaProjects/joinExperiments/resources/tpchQ3/customer.tbl").as[(String)]
    .map{line =>
      var fields = line.split('|')
      Customer(fields(0).toInt,fields(6))
    }
    .filter{cust => cust.mktSegment == "BUILDING"  }


  var order  =   spark.read.format("csv").load("file:///home/vinicius/IdeaProjects/joinExperiments/resources/tpchQ3/order.tbl").as[(String)]
    .map{line =>
      var fields = line.split('|')
      Order(fields(0).toInt, fields(1).toInt,
        Date.valueOf(fields(4)), fields(7).toInt)
    }
    .filter(order => order.orderDate.before(Date.valueOf("1995-03-15")))

  var lineItem  = spark.read.format("csv").load("file:///home/vinicius/IdeaProjects/joinExperiments/resources/tpchQ3/lineitem.tbl").as[(String)]
    .map{line =>
      var fields = line.split('|')
      LineItem(fields(0).toInt, fields(5).toDouble * (1 - fields(6).toDouble),Date.valueOf(fields(10)))
    }
    .filter(line => line.shipDate.after(Date.valueOf("1995-03-15")))


  var p = customer
      .join(order,customer("custKey") === order("custKey"))
      .join(lineItem,order("orderKey")  === lineItem("orderKey"))
      .count()

    println("Result size: " + p)

  //  println("Result size: " + order.count())
//  println("Result size: " + lineItem.count())


}
