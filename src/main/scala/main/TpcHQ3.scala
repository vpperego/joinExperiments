package main

import java.time.LocalDate

import main.startup.{config, spark}

case class Customer(custKey: Int, mktSegment: String)
case class Order(orderKey: Int,custKey: Int, orderDate: LocalDate, shipPriority: Int)
case class LineItem(orderKey: Int, revenue: Double, shipDate: LocalDate)
object TpcHQ3 {
  var sc = spark.sparkContext
  var customer  = sc.textFile(config("filePath"))
    .map{line =>
      var fields = line.split('|')
      Customer(fields(0).toInt,fields(6))
    }
    .filter{cust => cust.mktSegment == "BUILDING"  }
    .map(_.custKey)


  var order  =   sc.textFile(config("filePath"))
    .map{line =>
      var fields = line.split('|')
      Order(fields(0).toInt, fields(1).toInt, LocalDate.parse(fields(4)), fields(7).toInt)
    }
    .filter(order => order.orderDate.isBefore(LocalDate.parse("1995-03-13")))

  var lineItem  = sc.textFile(config("filePath"))
    .map{line =>
      var fields = line.split('|')
      LineItem(fields(0).toInt, fields(5).toDouble * (1 - fields(6).toDouble),LocalDate.parse(fields(10)))
    }
    .filter(line => line.shipDate.isAfter(LocalDate.parse("1995-03-19")))

  var broadOrder = sc.broadcast(order.collect)

   var joinedOrder =   customer.mapPartitions{ part =>
       part.flatMap(customerRow =>
         broadOrder.value.map{ orderTuple =>
           (customerRow, orderTuple)
         })
     }.filter{case (customer: Int, order: Order) => order.custKey== customer}.map(_._2)


  broadOrder = sc.broadcast(joinedOrder.collect())

  lineItem.mapPartitions{ part =>
    part.flatMap(storedTuple =>
      broadOrder.value.map{ broadcastTuple =>
        (broadcastTuple,storedTuple )
      })
  }.filter{case (order: Order, lineItem: LineItem) => order.orderKey == lineItem.orderKey}
    .map(_._1)
}
