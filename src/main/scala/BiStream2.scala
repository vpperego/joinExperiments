import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object BiStream2 {
  val spark = SparkSession
    .builder
      .master("local[*]")
    .appName("local joijdsaojdisa")
    .getOrCreate
  val sc = spark.sparkContext

  val relA = sc.parallelize(Array('a','b','c','d','e','f','g','h','i','j'))

  val relB = sc.parallelize(Array('k','l','m','n','o'))

  val relC = sc.parallelize(Array('k','l','m','n','o'))


  val parallelismForA = 3
  val rng = new scala.util.Random



  val parallelismForB = 2

  val storeA = relA.map { element => ((rng.nextInt(parallelismForA), 'A'), ('A', element)) }

  val storeB = relB.map { element => ((rng.nextInt(parallelismForB), 'B'), ('B', element)) }



  val probeB = relA.flatMap { element => (0 until parallelismForB).map { idx => ((idx, 'B'), ('A', element)) } }

  val probeA = relB.flatMap { element => (0 until parallelismForA).map { idx => ((idx, 'A'), ('B', element)) } }



  val totalA = storeA.union(probeA)

  val totalB = storeB.union(probeB)



  val resultA = totalA.groupByKey.flatMap(joinFunction)

  val resultB = totalB.groupByKey.flatMap(joinFunction)



  val result = resultA



  result.foreach(println)



  def joinFunction(arg: ((Int, Char), Iterable[(Char, Char)])): Iterable[(Char, Char)] = arg match {

    case(keypair: (Int, Char), values: Iterable[(Char, Char)]) => {

      // keypair is the partition id as well as the store relation.



      val storedTuples = values.filter(_._1 == keypair._2).map(_._2)

      val probedTuples = values.filter(_._1 != keypair._2).map(_._2)



      val result = ArrayBuffer[(Char, Char)]()

      for(stored <- storedTuples) {

        for(probe <- probedTuples) {

          if(predicate(stored, probe))

            result.append((stored, probe))

        }

      }

      result

    }

  }



  def predicate(left: Char, right: Char): Boolean = {

    "aeiou".indexOf(left) >= 0 && "aeiou".indexOf(right) >= 0

  }
}
