import org.apache.spark.sql.SparkSession

object kafkaProducer extends App{
  val spark = SparkSession
    .builder
    .appName("Kafka Producer")
    .getOrCreate

  var df = spark
    .read
    .csv("file:///home/vinicius/IdeaProjects/joinExperiments/resources/relA.csv");
  df.printSchema;

  df = df.withColumnRenamed("_c0","value");
  df.show(20)
  df.selectExpr("CAST(value AS STRING)")
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", "dbis-expsrv3:9092,dbis-expsrv8:9092,dbis-expsrv9:9092")
    .option("topic", "fooTopic")
    .save()

}
