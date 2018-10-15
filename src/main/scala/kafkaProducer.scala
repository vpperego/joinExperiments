import startup.{config, spark}

object kafkaProducer {
  import spark.implicits._

  def run: Unit = {
    var df = spark
      .read
      .csv(config("relASource"))

    df = df.withColumnRenamed("_c0", "value")
    df.show(20)
    df.selectExpr("CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", config("kafkaServer"))
      .option("topic", config("kafkaTopic"))
      .save
  }
}
