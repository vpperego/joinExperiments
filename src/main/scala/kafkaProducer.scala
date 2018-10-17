import startup.{config, spark}

object kafkaProducer {

  def run: Unit = {
    var df = spark
      .read
      .csv(config("relSource"))

    df
      .selectExpr("to_json(struct(*)) AS value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", config("kafkaServer"))
      .option("topic", config("kafkaTopic"))
      .save
  }
}
