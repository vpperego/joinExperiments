package dstream

 import main.startup.{config}
 import org.apache.kafka.common.serialization.StringDeserializer
 import org.apache.spark.streaming.StreamingContext
 import org.apache.spark.streaming.dstream.DStream
 import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
 import org.apache.spark.streaming.kafka010.KafkaUtils
 import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

class DStreamUtils {

   def createKafkaStream (ssc: StreamingContext,serverName: String,topicName: Array[String],groupName: String): DStream[Int] ={
        val kafkaParams = Map[String, Object](
          "bootstrap.servers" -> config("kafkaServer"),
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "group.id" -> groupName,
          "auto.offset.reset" -> "latest",
          "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        KafkaUtils.createDirectStream[String, String](
          ssc,
          PreferConsistent,
          Subscribe[String, String](topicName, kafkaParams)
        )      .map(row => row.value.toInt)
  }

   def joinCondition(pair: (Int,Int)): Boolean = {
    pair._1 < pair._2
   }

   def joinCondition2(pair: (Int,Int,Int)): Boolean = {
    pair._2 < pair._3
   }
}