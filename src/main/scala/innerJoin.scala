import org.apache.spark.sql.types.StructType
import startup.spark

object innerJoin extends App {

  import spark.implicits._

  val userSchema = new StructType().add("keyA", "integer")
  val userSchema2 = new StructType().add("keyB", "integer")

  var stream1 = spark
    .readStream
    .schema(userSchema)
    .csv("hdfs:/user/vinicius/relA")

  var stream2 = spark
    .readStream
    .schema(userSchema2)
    .csv("hdfs:/user/vinicius/relB")

  stream1
    .join(stream2, $"keyA" ===
      $"keyB")
    .writeStream
    .format("csv")
    .option("path", "/user/vinicius/output")
    .option("checkpointLocation", "/user/vinicius/checkpoint")
    .start
    .awaitTermination()
}
