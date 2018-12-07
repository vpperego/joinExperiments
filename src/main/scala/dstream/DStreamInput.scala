package dstream

import org.apache.kafka.common.serialization.StringDeserializer
import main.startup.{config, spark}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreamInput{


 def run: Unit ={
   val kafkaParams = Map[String, Object](
     "bootstrap.servers" -> config("kafkaServer"),
     "key.deserializer" -> classOf[StringDeserializer],
     "value.deserializer" -> classOf[StringDeserializer],
     "group.id" -> "use_a_separate_group_id_for_each_stream",
     "auto.offset.reset" -> "latest",
     "enable.auto.commit" -> (false: java.lang.Boolean)
   )
   val kafkaTopic = Array(config("kafkaTopic"))

   var sc = spark.sparkContext
   val ssc = new StreamingContext(sc, Seconds(10))
   val parallelismForA = 3
   val rng = new scala.util.Random


   var storeRddA:RDD[((Int,Char),(Char,String))] = sc.emptyRDD
   storeRddA = storeRddA.setName("Store - A")
   //Input stream that listen for new data
   val stream = KafkaUtils.createDirectStream[String, String](
     ssc,
     PreferConsistent,
     Subscribe[String, String](kafkaTopic, kafkaParams)
   )

   // Group incoming data (e.g., bi-stream style) and append to stored data
   // , returning a stream with everything that arrived so far.

   var storedStreamA =  stream
     .map(element =>  ((rng.nextInt(parallelismForA), 'A'), ('A', element.value())))
     .transform{newRdd =>
       storeRddA = storeRddA.union(newRdd)
       storeRddA.cache
     }

   // DO SOME JOIN OPERATION

   // Just print 100 top tuples (NOTE: when the stream has more than 100 tuples, incoming data will not be outputed)
   storedStreamA.print(100)

   //start the streaming context

   ssc.start()
   ssc.awaitTermination()
 }

}
