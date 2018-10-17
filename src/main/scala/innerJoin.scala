import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import startup.{config, spark}
import org.apache.spark.sql.functions._

object innerJoin {
  val userSchema = new StructType().add("keyA", "integer")
  val userSchema2 = new StructType().add("keyB", "integer")

  def run: Unit = {
    config("joinType") match {
      case "fileJoin" => fileJoin
      case "kafkaJoin" => kafkaJoin

    }
  }
  import spark.implicits._

  def fileJoin: Unit = {

    var stream1 = spark
      .readStream
      .schema(userSchema)
      .csv(config("relASource"))

    var stream2 = spark
      .readStream
      .schema(userSchema2)
      .csv(config("relBSource"))

    stream1
      .join(stream2, $"keyA" ===
        $"keyB")
      .writeStream
      .format("csv")
      .option("path", config("outputPath"))
      .option("checkpointLocation", config("checkpointPath"))
      .start
      .awaitTermination()
  }


  def kafkaJoin: Unit = {
    var relA = spark
      .readStream
      .format("kafka")
      .option("failOnDataLoss", "false")
      .option("kafka.bootstrap.servers", config("kafkaServerA"))
      .option("subscribe", config("kafkaTopicA"))
      .option("startingOffsets", "earliest")
      .load
      .selectExpr("CAST(value AS STRING)")
      .as[(String)]
      .select(from_json($"value", userSchema).as("data"))
      .select($"data.keyA")

    var relB = spark
      .readStream
      .format("kafka")
      .option("failOnDataLoss", "false")
      .option("kafka.bootstrap.servers", config("kafkaServerB"))
      .option("subscribe", config("kafkaTopicB"))
      .option("startingOffsets", "earliest")
      .load
      .selectExpr("CAST(value AS STRING)")
      .as[(String)]
      .select(from_json($"value", userSchema2).as("data2"))
      .select($"data2.keyB")

    relA.join(relB, $"keyA" === $"keyB")
      .selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config("kafkaServerOutput"))
      .option("topic", config("kafkaTopicOutput"))
      .option("checkpointLocation", config("checkpointPath"))
      .start
      .awaitTermination
  }

}
