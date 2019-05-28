package main

import main.startup.{config, spark}
import org.apache.log4j.{Level, Logger}

case class Supplier(suppKey: Int, nationKey: Int)
case class Nation(nationKey: Int, regionKey: Int)
case class Region(regionKey: Int)

object TpcHQ5Batch {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  println("Starting  TPC-H Q5 - Batch ")

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
    .map(fields => LineItem(fields(0).toInt,fields(2).toInt))
    .toDS()

  var supplier= spark.sparkContext.textFile(config("supplierPath"))
    .map(_.split('|'))
    .map(fields => Supplier(fields(0).toInt,  fields(3).toInt))
    .toDS()

//  var nation  = spark.sparkContext.textFile(config("nationPath"))
//    .map(_.split('|'))
//    .map(fields => Nation(fields(0).toInt,fields(2).toInt))
//    .toDS()
//
//  var region  = spark.sparkContext.textFile(config("regionPath"))
//    .map(_.split('|'))
//    .map(fields => Region(fields(0).toInt))
//    .toDS()

  var p = customer
    .join(orders,customer("custKey") === orders("custKey"))
    .join(lineItem,orders("orderKey")  === lineItem("orderKey"))
    .join(supplier,lineItem("suppKey")  === supplier("suppKey"))
//    .join(nation,supplier("nationKey")  === nation("nationKey"))
//    .join(region,nation("regionKey")  === region("regionKey"))
    .count()

  println("Result size: " + p)


}