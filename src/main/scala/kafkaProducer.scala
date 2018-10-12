import startup.spark

object kafkaProducer extends App{
  import spark.implicits._

  var df = spark
    .read
  .csv("hdfs:/user/vinicius/relA.csv")

  df = df.withColumnRenamed("_c0","value")
  df.show(20)
  df.selectExpr("CAST(value AS STRING)")
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", "dbis-expsrv1:9092")
    .option("topic", "fooTopic")
    .save
}
