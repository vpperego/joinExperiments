package structuredStreaming

import java.util.Properties

import main.startup.{config, spark}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.types.{IntegerType, StructType}

object kafkaProducer {
//  val userSchema = new StructType().add(config("joinKey"), IntegerType)

  def run(): Unit = {
//    var df = spark
//      .read
//      //      .schema(userSchema)
//      .csv(config("relSource"))

    var df = spark
      .sparkContext
        .textFile(config("relSource"))


    df.foreachPartition { resultPartition =>
      val kafkaProps = new Properties();
      kafkaProps.put("bootstrap.servers", "dbis-expsrv1:9092");
      kafkaProps.put("client.id", "KafkaIntegration Producer");
      kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
      kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
      val producer = new KafkaProducer[String, String](kafkaProps);
      resultPartition.foreach { resultTuple =>
        var resString = resultTuple.toString;
        //._1 + "," + resultTuple._2
         val message = new ProducerRecord[String, String]("intDs", resString, resString);
        producer.send(message)
      }
      producer.close()
      //      }
    }

  }
//
//      .selectExpr("to_json(struct(*)) AS value")
//      .write
//      .format("kafka")
//      .option("kafka.bootstrap.servers", config("kafkaServer"))
//      .option("topic", config("kafkaTopic"))
//      .save

}
