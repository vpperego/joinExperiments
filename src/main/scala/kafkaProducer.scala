import org.apache.spark.sql.types.{IntegerType, StructType}
import startup.{config, spark}

object kafkaProducer {
  val userSchema = new StructType().add("keyB", IntegerType)

  def run: Unit = {
    var df = spark
      .read
      .schema(userSchema)
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
