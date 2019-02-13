package main

import java.sql.Date

import main.startup.spark

case class Customer(cKey: Int, mktSegment: String)
case class Order(orderKey: Int,custKey: Int, orderDate: Date, shipPriority: Int)
case class LineItem(oKey: Int, revenue: Double, shipDate: Date)

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
    .filter(order => order.orderDate.before(Date.valueOf("1995-03-13")))

  var lineItem  = spark.read.format("csv").load("file:///home/vinicius/IdeaProjects/joinExperiments/resources/tpchQ3/lineitem.tbl").as[(String)]
    .map{line =>
      var fields = line.split('|')
      LineItem(fields(0).toInt, fields(5).toDouble * (1 - fields(6).toDouble),Date.valueOf(fields(10)))
    }
    .filter(line => line.shipDate.after(Date.valueOf("1995-03-19")))


  var p = customer
      .join(order,$"cKey" === $"custKey")
      .join(lineItem,$"orderKey" === $"oKey")
      .count()

  println("Result size: " + p)//Result size: 27443 vs stream 29412

//  println("Result size: " + order.count())
//  println("Result size: " + lineItem.count())

//  var broadCustomer: Broadcast[Array[Customer]] = sc.broadcast(customer.collect)
//
//   var intermediateResult =   order.mapPartitions{ part =>
//       part.flatMap(orderTuple =>
//         broadCustomer.value.map{ customerTuple =>
//           (customerTuple, orderTuple)
//         })
//     }
//     .filter( joinedTuple => joinedTuple._1.custKey == joinedTuple._2.custKey)
//
//
//	println("Intermediate result: " + intermediateResult.count())
//
//	var broadcastIntermediate = sc.broadcast(intermediateResult.collect)
//
//	var result = lineItem.mapPartitions{ part =>
//       part.flatMap(lineItemTuple =>
//         broadcastIntermediate.value.map{intermediateTuple =>
//           (intermediateTuple, lineItemTuple)
//         }
//       )
//  }
//  .filter(tuple => tuple._1._2.orderKey == tuple._2.orderKey)

//  println(s"Final result size ${result.count}")

}
