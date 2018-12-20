package structuredStreaming

import main.startup.{spark, config}

import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{IntegerType, StructType}

object innerJoin {
  val userSchema = new StructType().add("keyA", IntegerType)
  val userSchema2 = new StructType().add("keyB", IntegerType)

  def run: Unit = {
    config("joinType") match {
      case "fileJoin" => fileJoin
      case "kafkaJoin" => kafkaJoin

    }
  }

  def fileJoin: Unit = {
    import spark.implicits._
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
    import spark.implicits._
    var relA = spark
      .readStream
      .format("kafka")
      .option("failOnDataLoss", "false")
      .option("kafka.bootstrap.servers", config("kafkaServerA"))
      .option("subscribe", config("kafkaTopicA"))
      .option("startingOffsets", "earliest")
//      .option("maxOffsetsPerTrigger", "1000")
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
//      .option("maxOffsetsPerTrigger", "1000")
      .load
      .selectExpr("CAST(value AS STRING)")
      .as[(String)]
      .select(from_json($"value", userSchema2).as("data2"))
      .select($"data2.keyB")

      relA.join(relB, $"keyA" < $"keyB")
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
