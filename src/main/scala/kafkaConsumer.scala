import org.apache.spark.sql.SparkSession

object kafkaConsumer extends App{
  val spark = SparkSession
    .builder
    .appName("Kafka Consumer")
    .getOrCreate
  import spark.implicits._

  var df = spark
    .read
    .format("kafka")
    .option("kafka.bootstrap.servers", "dbis-expsrv3:9092,dbis-expsrv8:9092,dbis-expsrv9:9092")
    .option("subscribe", "fooTopic")
    .load()

  df.selectExpr("CAST(value AS STRING)")
    .as[String]
    .show(20)
 
}
