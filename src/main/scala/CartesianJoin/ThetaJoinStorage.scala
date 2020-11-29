package CartesianJoin

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

class ThetaJoinStorage[T: ClassTag] (sc:SparkContext){
  private var storeRdd: RDD[T] = sc.emptyRDD

  def store (source: DStream[T]): DStream[T] = {
    source.transform{ streamedRdd =>
        storeRdd = storeRdd.union(streamedRdd)
        storeRdd = storeRdd.cache()
        streamedRdd
    }
  }

  def join [U: ClassTag] (@transient otherRelation:DStream[U],  joinCondition: (T,U) => Boolean): DStream[(T, U)] = {
    import CartesianJoin.ThetaJoin._
    otherRelation.transform(streamedRdd =>
      storeRdd.thetaJoin(streamedRdd, joinCondition)
    )
  }

  def joinAsRight [U: ClassTag] (@transient otherRelation:DStream[U],  joinCondition: (U,T) => Boolean): DStream[(U, T)] = {
    import CartesianJoin.ThetaJoin._
    otherRelation.transform(streamedRdd =>
      streamedRdd.thetaJoin(storeRdd, joinCondition)
    )
  }
}
