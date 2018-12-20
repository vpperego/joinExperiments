package structuredStreaming

import org.apache.spark.sql.functions.from_json
import structuredStreaming.innerJoin.userSchema
import main.startup.{config, spark}
import org.apache.spark.sql.Dataset

object kafkaConsumer {

  def run: Unit = {
    import spark.implicits._

    var df = spark
    .read
    .format("kafka")
      .option("kafka.bootstrap.servers", config("kafkaServer"))
      .option("subscribe", config("kafkaTopic"))
    .load()

    var foo: Dataset[Int] = df.selectExpr("CAST(value AS STRING)")
      .as[(String)]
      .map{_.toInt}


//      .select(from_json($"value", userSchema).as("data"))
//    .show(20)
  }

}
