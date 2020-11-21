package dstream

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class ThetaJoinPartition(
                          idx: Int,
                          @transient private val rdd1: RDD[_],
                          @transient private val rdd2: RDD[_],
                          s1Index: Int,
                          s2Index: Int
                        ) extends Partition {
  var s1 = rdd1.partitions(s1Index)
  var s2 = rdd2.partitions(s2Index)
  override val index: Int = idx

}

class ThetaJoinRDD(sc: SparkContext, var rdd1 : RDD[Int], var rdd2 : RDD[Int], joinPredicate: (Int, Int) => Boolean) extends RDD[(Int, Int)](sc, Nil) with Serializable
{
  val numPartitionsInRdd2 = rdd2.partitions.length

  override def compute(split: Partition, context: TaskContext): Iterator[(Int, Int)] = {
    val currSplit = split.asInstanceOf[ThetaJoinPartition]
    rdd1.iterator(currSplit.s1, context).flatMap{x =>
      rdd2.iterator(currSplit.s2, context).flatMap{ y=>
        Option(if(joinPredicate(x, y)) (x, y) else null)
      }
    }
  }

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](rdd1.partitions.length * rdd2.partitions.length)
    for (s1 <- rdd1.partitions; s2 <- rdd2.partitions) {
      val idx = s1.index * numPartitionsInRdd2 + s2.index
      array(idx) = new ThetaJoinPartition(idx, rdd1, rdd2, s1.index, s2.index)
    }
    array
  }
}


class ThetaJoin(rdd: RDD[Int]) {
  def thetaJoin(otherRdd: RDD[Int], joinPredicate: (Int, Int) => Boolean) = {
    new ThetaJoinRDD(SparkSession.getActiveSession.get.sparkContext, rdd, otherRdd, joinPredicate)
  }
}

object ThetaJoin {
  implicit def addCustomFunctions(rdd: RDD[Int]) = new
      ThetaJoin(rdd)
}