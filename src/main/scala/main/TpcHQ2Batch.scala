package main

import java.sql.Date

import main.startup.{config, spark}
import org.apache.log4j.{Level, Logger}



case class Part(partKey: Int)
case class PartSupp(partKey: Int, suppKey: Int)
case class Nation(nationKey: Int, regionKey: Int)
case class Region(regionKey: Int)

object TpcHQ2Batch {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  println("Starting  TPC-H Q3 - Batch ")

  import spark.implicits._


  var part= spark.sparkContext.textFile(config("partPath"))
    .map(_.split('|'))
    .map(fields => Part(fields(0).toInt))
    .toDS()


  var partSupp= spark.sparkContext.textFile(config("partSuppPath"))
    .map(_.split('|'))
    .map(fields => PartSupp(fields(0).toInt,  fields(1).toInt))
    .toDS()

  var supplier= spark.sparkContext.textFile(config("supplierPath"))
    .map(_.split('|'))
    .map(fields => Supplier(fields(0).toInt,  fields(3).toInt))
    .toDS()

  var nation  = spark.sparkContext.textFile(config("nationPath"))
    .map(_.split('|'))
    .map(fields => Nation(fields(0).toInt,fields(2).toInt))
    .toDS()

  var region  = spark.sparkContext.textFile(config("regionPath"))
    .map(_.split('|'))
    .map(fields => Region(fields(0).toInt))
    .toDS()

//Part|x|Partsupp|x|Supplier|x|Nation|x|Region
  var p =
    part
    .join(partSupp,part("partKey") === partSupp("partKey"))
    .join(supplier,partSupp("suppKey")  === supplier("suppKey"))
    .join(nation,supplier("nationKey") === nation("nationKey"))
    .join(region,nation("regionKey")  === region("regionKey"))
    .count()


  println("Result size: " + p)
}
