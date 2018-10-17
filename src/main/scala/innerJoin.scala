import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import startup.{config, spark}

object innerJoin {

  def run: Unit = {
    config("joinType") match {
      case "fileJoin" => fileJoin
      case "kafkaJoin" => kafkaJoin

    }
  }
  import spark.implicits._

  def fileJoin: Unit = {
    val userSchema = new StructType().add("keyA", "integer")
    val userSchema2 = new StructType().add("keyB", "integer")

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
      .as[String]


    var relB = spark
      .readStream
      .format("kafka")
      .option("failOnDataLoss", "false")
      .option("kafka.bootstrap.servers", config("kafkaServerB"))
      .option("subscribe", config("kafkaTopicB"))
      .option("startingOffsets", "earliest")
      .load
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .withColumnRenamed("value", "_value")

    relA
      .join(relB, $"_value" === $"value")
      .writeStream
      .format("kafka")

      .option("kafka.bootstrap.servers", config("kafkaServerOutput"))
      .option("topic", config("kafkaTopicOutput"))
      .option("checkpointLocation", config("checkpointPath"))
      .start
      .awaitTermination
  }

}
