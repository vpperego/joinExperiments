package structuredStreaming

import main.startup.{spark, config}

object kafkaProducer {
//  val userSchema = new StructType().add(config("joinKey"), IntegerType)

  def run: Unit = {
    var df = spark
      .read
//      .schema(userSchema)
      .csv(config("relSource"))

    df
//      .selectExpr("to_json(struct(*)) AS value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", config("kafkaServer"))
      .option("topic", config("kafkaTopic"))
      .save
  }
}
