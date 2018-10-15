import org.apache.spark.sql.types.StructType
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

  }

}
