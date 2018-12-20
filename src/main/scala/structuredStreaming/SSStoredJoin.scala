package structuredStreaming

import main.startup.{config, spark}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Dataset


case class Course(Id: Int)
 case class groupedRows(groupId: Int, row: Int)
object SSStoredJoin {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  import spark.implicits._

  val relASchema = ScalaReflection.schemaFor[Course].dataType.asInstanceOf[StructType]

//  var relA: Dataset[Int] = createStructuredStreamingSource(config("kafkaTopicA"),config("kafkaServer"))
  var relB = createStructuredStreamingSource(config("kafkaTopicB"),config("kafkaServer"))
//  var relC = createStructuredStreamingSource(config("kafkaTopicC"),config("kafkaServer"))

//  val storeA = new Storage(spark,"relA")
  val storeB = new Storage(spark,"relB")

  while(true){
//    storeA.store(relA,2000)
    storeB.store(relB,6000)

//    storeA.join(relB)
//    storeB.join(relA)

//    var sA = storeA.join(relB)
//    var sB = storeB.join(relA)
//
//    var result = sA.union(sB)
//
//    result
//      .writeStream
//      .format("console")
//      .start
//      .awaitTermination(8000)

  }


  private def createStructuredStreamingSource (topicName: String, serverName: String): Dataset[Int] ={
    import spark.implicits._
      spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", serverName)
      .option("subscribe", topicName)
      .load
      .selectExpr("CAST(value AS STRING)")
      .as[(String)]
      .map{_.toInt}
   }
}
