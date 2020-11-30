package test

import org.apache.spark.sql.SparkSession

object ThetaJoinTest {
  import CartesianJoin.ThetaJoin._
  private val sc = SparkSession.getActiveSession.get.sparkContext

  def IntTest() = {
    import CartesianJoin.ThetaJoin._

    val arr1 = Array(1, 2, 3)
    val arr2 = Array(3, 2,1)
    val sc = SparkSession.getActiveSession.get.sparkContext
    val rd1 = sc.parallelize(arr1)
    val rd2 = sc.parallelize(arr2)
    val cond = (x: Int, y: Int) => x >= y
    val finalDf = rd1.thetaJoin(rd2, cond)
    finalDf.collect.foreach(println)
  }

}
