package dstream

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag

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

class ThetaJoinRDD [T: ClassTag, U: ClassTag](sc: SparkContext, var rdd1 : RDD[T], var rdd2 : RDD[U], joinPredicate: (T, U) => Boolean) extends RDD[(T, U)](sc, Nil) with Serializable
{
  val numPartitionsInRdd2 = rdd2.partitions.length

  override def compute(split: Partition, context: TaskContext): Iterator[(T, U)] = {
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

class ThetaJoin[T: ClassTag](rdd: RDD[T]) extends Serializable {
  def thetaJoin[U: ClassTag](otherRdd: RDD[U], joinPredicate: (T, U) => Boolean): ThetaJoinRDD[T, U] = {
    new ThetaJoinRDD(SparkSession.getActiveSession.get.sparkContext, rdd, otherRdd, joinPredicate)
  }
}

object ThetaJoin extends Serializable {
  implicit def addThetaJoinFunction[T: ClassTag] (rdd: RDD[T]) = new
      ThetaJoin(rdd)
}