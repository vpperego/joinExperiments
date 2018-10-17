import innerJoin.userSchema
import org.apache.spark.sql.functions.from_json
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
      .as[(String)]
      .select(from_json($"value", userSchema).as("data"))
    .show(20)
  }

}
