import java.io.PrintWriter


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.io
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

object startup extends App {

  val spark = SparkSession
    .builder
    .appName(_appName)
    .getOrCreate
  val config = spark.sparkContext.textFile(args(1))
    .map(_.split("[\t ]+"))
    .map(arr => arr(0) -> arr(1))
    .collect
    .toMap


  spark.streams.addListener(new StreamingQueryListener() {
    override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {

    }

    override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
    }

    override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
      val conf = new Configuration()
      val fs = FileSystem.get(conf)
      fs.mkdirs(
        new Path("hdfs:/user/vinicius/logs/" + _appName))
      val output = fs.create(
        new Path("hdfs:/user/vinicius/logs/" +
          _appName + "/" + queryProgress.progress.batchId + ".json"))

      val writer = new PrintWriter(output)
      writer.write(queryProgress.progress.json)
      writer.close()

    }
  })
  var _appName = args(0)


  config("class") match {
    case "innerJoin" => innerJoin.run
    case "kafkaConsumer" => kafkaConsumer.run
    case "kafkaProducer" => kafkaProducer.run
    case _ => None
  }

}
