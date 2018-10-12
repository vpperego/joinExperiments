import startup.spark

object kafkaConsumer extends App{
  import spark.implicits._

  var df = spark
    .read
    .format("kafka")
    .option("kafka.bootstrap.servers", "dbis-expsrv1:9092")
    .option("subscribe", "fooTopic")
    .load()

  df.selectExpr("CAST(value AS STRING)")
    .as[String]
    .show(20)
}
