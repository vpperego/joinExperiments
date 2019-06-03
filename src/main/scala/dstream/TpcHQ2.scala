package dstream

import main.startup.{config, spark}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

case class Part(partKey: Int)
case class PartSupp(partKey: Int, suppKey: Int)
case class Nation(nationKey: Int, regionKey: Int)
case class Region(regionKey: Int)

object TpcHQ2 {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  var sc = spark.sparkContext
  sc.getConf.registerKryoClasses(Array(classOf[Customer], classOf[Order], classOf[LineItem], classOf[(Customer, Order)]))

  val ssc = new StreamingContext(sc, Seconds(12))

  var utils = new DStreamUtils


  var part: DStream[Part] = utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("part"), "part", true)
    .map(_.split('|'))
    .map(fields => Part(fields(0).toInt))


  var partSupp = utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("partsupp"), "partsupp", true)
    .map(_.split('|'))
    .map(fields => PartSupp(fields(0).toInt,  fields(1).toInt))

  var supplier: DStream[Supplier] = utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("supplier"), "supplier", true)
    .map(_.split('|'))
    .map(fields => Supplier(fields(0).toInt, fields(3).toInt))

  var nation  = utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("nation"), "nation", true)
    .map(_.split('|'))
    .map(fields => Nation(fields(0).toInt,      fields(2).toInt))

  var region  = utils.createKafkaStreamTpch(ssc, config("kafkaServer"), Array("region"), "region", true)
    .map(_.split('|'))
    .map(fields => Region(fields(0).toInt))


  // STORAGES
  var partStorage = new GenericStorage[Part](sc, "part")
  var partSuppStorage = new GenericStorage[PartSupp](sc, "partSupp")
  var supplierStorage = new GenericStorage[Supplier](sc, "supplier")
  var nationStorage = new GenericStorage[Nation](sc, "nation")
  var regionStorage = new GenericStorage[Region](sc, "region")
  var partsIntermediateStorage = new GenericStorage[((Part, PartSupp), Long)](sc, "P'PS-Intermediate")
  var partsSuppIntermediateStorage = new GenericStorage[((((Part, PartSupp), Long), Supplier), Long)](sc, "P'PS-Intermediate")
  var partsSuppNationIntermediateStorage = new GenericStorage[((((((Part, PartSupp), Long), Supplier), Long), Nation), Long)](sc, "P'PS'S-Intermediate")

  // PROBED STREAMS
  var probedPart = partStorage.store(part)
  var probedPartSupp = partSuppStorage.store(partSupp)
  var probedSupplier = supplierStorage.store(supplier)
  var probedNation = nationStorage.store(nation)
  var probedRegion = regionStorage.store(region)


  //JOIN PREDICATES
  val partJoinPredicate = (pair:((Part, Long),(PartSupp, Long))) => pair._1._1.partKey == pair._2._1.partKey && pair._1._2 < pair._2._2
  val partSuppJoinPredicate = (pair:((Part, Long),(PartSupp, Long))) => pair._1._1.partKey == pair._2._1.partKey && pair._2._2 < pair._1._2
  val partsIntermediateJoinPredicate= (pair:( (((Part, PartSupp), Long), Long),(Supplier, Long))) =>  pair._1._1._1._2.suppKey == pair._2._1.suppKey &&  pair._1._2 < pair._2._2
  val supplierJoinPredicate= (pair:( (((Part, PartSupp), Long), Long),(Supplier, Long))) =>  pair._1._1._1._2.suppKey == pair._2._1.suppKey &&  pair._2._2 < pair._1._2
  val partsSuppJoinPredicate= (pair:( (((((Part, PartSupp), Long), Supplier), Long), Long),(Nation, Long))) =>  pair._1._1._1._2.nationKey == pair._2._1.nationKey &&  pair._1._2 < pair._2._2
  val nationJoinPredicate= (pair:( (((((Part, PartSupp), Long), Supplier), Long), Long),(Nation, Long))) =>  pair._1._1._1._2.nationKey == pair._2._1.nationKey &&  pair._2._2 < pair._1._2
  val partsSuppNationJoinPredicate= (pair:( (((((((Part, PartSupp), Long), Supplier), Long), Nation), Long), Long),(Region, Long))) =>  pair._1._1._1._2.regionKey == pair._2._1.regionKey &&  pair._1._2 < pair._2._2
  val regionJoinPredicate =         (pair:( (((((((Part, PartSupp), Long), Supplier), Long), Nation), Long), Long),(Region, Long))) =>  pair._1._1._1._2.regionKey == pair._2._1.regionKey &&  pair._2._2 < pair._1._2

  var partJoinResult  = partStorage.join(probedPartSupp,partJoinPredicate,0L)
  var partSuppJoinResult  = partSuppStorage.joinAsRight(probedPart,partSuppJoinPredicate,0L)

  var partsResult: DStream[((Part, PartSupp), Long)] = partJoinResult.union(partSuppJoinResult) .map(coRow => ((coRow._1._1, coRow._1._2),coRow._3))
  var probedIntermediateParts = partsIntermediateStorage.store(partsResult)

  var intermediateJoinResult: DStream[((((Part, PartSupp), Long), Supplier), Long, Long)] = partsIntermediateStorage
    .join(probedSupplier, partsIntermediateJoinPredicate,0L)

  var supplierJoinResult: DStream[((((Part, PartSupp), Long), Supplier), Long, Long)] =
    supplierStorage.joinAsRight(probedIntermediateParts,supplierJoinPredicate,0L)

 var partsSuppResult: DStream[((((Part, PartSupp), Long), Supplier), Long)] = intermediateJoinResult
      .union(supplierJoinResult)
    .map(outputRow => (outputRow._1,outputRow._3))

 var probedpartsSupp: DStream[(((((Part, PartSupp), Long), Supplier), Long), Long)] = partsSuppIntermediateStorage.store(partsSuppResult)


  var partSuppNationIntermediateResult = partsSuppIntermediateStorage.join(probedNation, partsSuppJoinPredicate, 0L)

  var nationJoinResult = nationStorage
    .joinAsRight(probedpartsSupp,nationJoinPredicate,0L)

  var partSuppNationResult: DStream[((((((Part, PartSupp), Long), Supplier), Long), Nation), Long)] = partSuppNationIntermediateResult
    .union(nationJoinResult)
    .map(outputRow => (outputRow._1,outputRow._2))


  var probedpartsSuppNation= partsSuppNationIntermediateStorage.store(partSuppNationResult)

  var finalIntermediateResult = partsSuppNationIntermediateStorage.join(probedRegion,partsSuppNationJoinPredicate, 0L)
  var regionJoinResult = regionStorage.joinAsRight(probedpartsSuppNation, regionJoinPredicate,0L)

  var result = finalIntermediateResult
    .union(regionJoinResult)
      .map(outputRow => (outputRow._1._1._1._1._1._1._2,outputRow._1._1._1._1._2,outputRow._1._1._2,outputRow._3))

  result
    .saveAsTextFiles(config("hadoopFileName")+"/" +sc.applicationId+ "/")
  println("Waiting for jobs (TPC-H Q2)")

  ssc.start
  ssc.awaitTerminationOrTimeout(Minutes(config("waitingTime").toInt).milliseconds)
}
