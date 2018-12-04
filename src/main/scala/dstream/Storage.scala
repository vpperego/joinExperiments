package dstream

 import org.apache.spark.SparkContext
 import org.apache.spark.rdd.RDD
 import org.apache.spark.streaming.dstream.DStream

class Storage (sc:SparkContext) {
  private var storeRdd: RDD[Int] = sc.emptyRDD


   def store (source: DStream[Int]): DStream[Int] = {
     source.transform{newRdd =>
      storeRdd = storeRdd.union(newRdd)
        storeRdd.cache
    }
  }

  def join(rightRel: DStream[Int]): DStream[(Int,Int)] = {
    rightRel.transform{ thisRdd =>
      thisRdd.cartesian(storeRdd)
    }
  }
}
