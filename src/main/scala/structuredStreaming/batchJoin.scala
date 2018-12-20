package structuredStreaming

import main.startup.{config, spark}

object batchJoin {
  def run: Unit = {
    import spark.implicits._

    case class relA(keyA: Long)
    case class relB(keyB: Long)

    var relationA = spark
      .read
      .format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "false")
      .load(config("relASource"))
      .withColumnRenamed("_c0","keyA")

    //      .as[relA]

    relationA.printSchema()
    var relationB = spark
      .read
      .format("csv")
      .option("sep", ";")
      .option("header", "false")
      .option("inferSchema", "true")
      .load(config("relBSource"))
        .withColumnRenamed("_c0","keyB")
//      .as[relBStream]
    relationB.printSchema()

    relationA.join(relationB, $"keyA" < $"keyB")
      .write
      .format("csv")
      .save(config("outputPath"))

  }
}
