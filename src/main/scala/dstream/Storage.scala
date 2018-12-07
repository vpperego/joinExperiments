package dstream

 import org.apache.spark.SparkContext
 import org.apache.spark.rdd.RDD
 import org.apache.spark.streaming.dstream.DStream

 import org.apache.spark.streaming.StreamingContext
class Storage (sc:SparkContext, ssc: StreamingContext, storeName: String) {
  private var storeRdd: RDD[Int] = sc.emptyRDD
  private var intermediateStore: RDD[(Int,Int)] = sc.emptyRDD


   def store (source: DStream[Int]): Unit = {
     source.foreachRDD{newRdd =>
         storeRdd = storeRdd.union(newRdd)
         storeRdd.cache
     }
  }

  def storeIntermediateResult (source: DStream[(Int,Int)]): Unit = {
    source.foreachRDD{newRdd =>
      intermediateStore = intermediateStore.union(newRdd)
      intermediateStore.cache
    }
  }

  def join(rightRel: DStream[Int]): DStream[(Int,Int)] = {
    rightRel.transform{ streamRdd =>

      streamRdd.cartesian(storeRdd)
    }
  }

  def intermediateStoreJoin(rightRel: DStream[Int]): DStream[(Int,(Int, Int))] = {
    rightRel.transform{ streamRdd =>
      streamRdd.cartesian(intermediateStore)
    }
  }

  def joinWithIntermediateResult(rightRel: DStream[(Int,Int)]): DStream[(Int,(Int, Int))] = {
    rightRel.transform{ streamRdd =>

      storeRdd.cartesian(streamRdd)
    }
  }


}
