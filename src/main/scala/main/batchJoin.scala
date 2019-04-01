package main

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD

import startup.{config, spark}

object batchJoin {
  def run: Unit = {
      var relA = spark.sparkContext.textFile(config("relASource")).map(r => (r.toInt,System.currentTimeMillis()))
      var relB = spark.sparkContext.textFile(config("relBSource")).map(r => (r.toInt,System.currentTimeMillis()))
      var relC = spark.sparkContext.textFile(config("relCSource")).map(r => (r.toInt,System.currentTimeMillis()))

      var broadB=    spark.sparkContext.broadcast(relB.collect())
      var broadC=    spark.sparkContext.broadcast(relC.collect())


    var intermediateResult: RDD[(Int, Long)] = relA.mapPartitions{ partA =>
      partA.flatMap{rowA=>
        broadB.value.map(rowB => (rowA,rowB))
      }
    }.filter{case (a,b)=> a._1 < b._1}
      .map{joinedRow => (joinedRow._2._1,if (joinedRow._1._2 < joinedRow._2._2) joinedRow._1._2 else joinedRow._2._2)}


    var finalResult = intermediateResult.mapPartitions{ joinedPartition =>

      joinedPartition.flatMap{joinedRow =>
        broadC.value.map(rowC => (joinedRow,rowC))
      }
    }.filter{case (a,b)=> a._1 < b._1}
      .map(row => (if(row._1._2< row._2._2) row._1._2 else row._2._2,System.currentTimeMillis()))


    var startTime =  finalResult.keys.min()
    var endTime   =  finalResult.values.max()

    var total = finalResult.count

    val props = new Properties()
    props.put("bootstrap.servers", config("kafkaServer"))
    props.put("client.id", "kafkaProducer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    var time =endTime-startTime
    println( s"Result size: $total")
    println( s"Time = $time ms" )
    println( s"Troughput = ${total/time} ms" )

    //outputSize,Time,troughput,
    val msg = total.toString +"," + time.toString +","+(total/time)
    val data = new ProducerRecord[String, String](config("kafkaTopic"),
      config("kafkaServer"), msg)
    producer.send(data)
    producer.close
  }
}
