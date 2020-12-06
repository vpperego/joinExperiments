package util

case class Customer(custKey: Int)
case class Order(orderKey: Int,custKey: Int)
case class LineItem(orderKey: Int,suppKey: Int)