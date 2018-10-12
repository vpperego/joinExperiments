import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

object startup {
  val spark = SparkSession
    .builder
    .appName("Kafka Consumer")
    .getOrCreate


  spark.streams.addListener(new StreamingQueryListener() {
    override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {

    }

    override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
    }

    override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
      val conf = new Configuration()
      val fs = FileSystem.get(conf)
      val output = fs.create(
        new Path("hdfs:/user/vinicius/logs/" + queryProgress.progress.batchId + ".json"))
      val writer = new PrintWriter(output)
      writer.write(queryProgress.progress.json)
      writer.close()

    }
  })
}
