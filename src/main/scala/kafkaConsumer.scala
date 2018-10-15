import startup.{config, spark}

object kafkaConsumer {
  import spark.implicits._

  def run: Unit = {
    var df = spark
    .read
    .format("kafka")
      .option("kafka.bootstrap.servers", config("kafkaServer"))
      .option("subscribe", config("kafkaTopic"))
    .load()

    df.selectExpr("CAST(value AS STRING)")
    .as[String]
    .show(20)
  }

}
