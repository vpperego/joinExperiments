import java.io.PrintWriter


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.io
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

object startup extends App {
  println("\n\n\nArgs 0 =" + args(0) + "\n\n\n")

  val spark = SparkSession
    .builder
    .appName(args(0))
    .getOrCreate


  spark.streams.addListener(new StreamingQueryListener() {
    override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {

    }

    override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
    }

    override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
      //      val conf = new Configuration()
      //      val fs = FileSystem.get(conf)
      //      val output = fs.create(
      //        new Path("hdfs:/user/vinicius/logs/" + queryProgress.progress.batchId + ".json"))
      //      val writer = new PrintWriter(output)
      //      writer.write(queryProgress.progress.json)
      //      writer.close()

    }
  })


  val config = io
    .Source
    .fromFile(args(1))
    .mkString
    .split("\n")
    .map(_.split("\t"))
    .map(arr => arr(0) -> arr(1))
    .toMap


  config("class") match {
    case "innerJoin" => innerJoin.run
    case "kafkaConsumer" => kafkaConsumer.run
    case "kafkaProducer" => kafkaProducer.run
    case _ => None
  }

}
