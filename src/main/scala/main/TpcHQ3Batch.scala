package main

import java.sql.Date

import main.startup.{config, spark}

case class Customer(custKey: Int)
case class Order(orderKey: Int,custKey: Int)
case class LineItem(orderKey: Int)

object TpcHQ3Batch {

  import spark.implicits._


  var customer = spark.sparkContext.textFile(config("customerPath"))
    .map(_.split('|'))
    .map(fields => Customer(fields(0).toInt))
    .toDS()

  var orders = spark.sparkContext.textFile(config("orderPath"))
    .map(_.split('|'))
    .map(fields =>  Order(fields(0).toInt, fields(1).toInt))
    .toDS()
  var lineItem = spark.sparkContext.textFile(config("lineItemPath"))
    .map(_.split('|'))
    .map(fields => LineItem(fields(0).toInt))
    .toDS()


  var p = customer
    .join(orders,customer("custKey") === orders("custKey"))
    .join(lineItem,orders("orderKey")  === lineItem("orderKey"))
    .count()


    println("Result size: " + p)
}
