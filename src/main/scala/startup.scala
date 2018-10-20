import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

object startup extends App {
  var _appName = args(0)
  val LOG_PATH = "hdfs:/user/vinicius/logs/" + _appName

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
        new Path(LOG_PATH))
      val output = fs.create(
        new Path(LOG_PATH + "/"
          + queryProgress.progress.batchId + ".json"))

      val writer = new PrintWriter(output)
      writer.write(queryProgress.progress.json)
      writer.close()
    }
  })


  config("class") match {
    case "innerJoin" =>
      spark.conf.set("spark.sql.forceCrossJoin", config("forceCrossJoin"))
      innerJoin.run
    case "kafkaConsumer" => kafkaConsumer.run
    case "kafkaProducer" => kafkaProducer.run
    case _ => None
  }

}
