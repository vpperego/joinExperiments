import org.scalatest.FunSuite

import scala.collection.mutable
import scala.io.Source

class configParserTest extends FunSuite {

  test("simpe parser") {
    var sampleMap = new mutable.HashMap[String, String]()

    sampleMap.put("class", "innerJoin")
    sampleMap.put("joinType", "kafkaJoin")
    sampleMap.put("kafkaServerA", "dbis-expsrv1:9092")
    sampleMap.put("kafkaTopicA", "relA")
    sampleMap.put("kafkaServerB", "dbis-expsrv1:9092")
    sampleMap.put("kafkaTopicB", "relB")
    sampleMap.put("kafkaServerOutput", "dbis-expsrv1:9092")
    sampleMap.put("kafkaTopicOutput", "joinTestOutput")
    sampleMap.put("checkpointPath", "hdfs:/user/vinicius/checkpoint")

    var generatedMap = Source
      .fromFile("/home/vinicius/IdeaProjects/joinExperiments/resources/config/kafka-inner-join.conf")
      .mkString
      .split("\n")
      .map(_.split("[\t ]+"))
      .map(arr => arr(0) -> arr(1))
      .toMap

    generatedMap.foreach { case (key, value) =>
      println("Key: " + key + ", value: " + value)
      println("Sample value :" + sampleMap(key))
      assert(sampleMap(key) == value)
    }


  }
}
