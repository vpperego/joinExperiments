package dstream

 import org.apache.spark.SparkContext

 import org.apache.spark.rdd.RDD
 import org.apache.spark.streaming.dstream.DStream
 import org.apache.spark.streaming.StreamingContext
class Storage (sc:SparkContext, ssc: StreamingContext, storeName: String) {
  private var storeRdd: RDD[Int] = sc.emptyRDD
   private var intermediateStore: RDD[(Int,Int)] = sc.emptyRDD

   def store (source: DStream[Int]): Unit = {
     source.foreachRDD{ streamedRdd =>
       if(!streamedRdd.isEmpty()){
//         var oldStore = storeRdd
         storeRdd = storeRdd.union(streamedRdd)
//           .repartition(8)
           .setName(storeName)
         println("Size of "+storeName + " = " + storeRdd.count())
         storeRdd.cache()
//         oldStore.unpersist()
       }
     }
  }

  def storeIntermediateResult (source: DStream[(Int,Int)]): Unit = {
    source.foreachRDD{ intermediateStreamedRdd =>
      if(!intermediateStreamedRdd.isEmpty()){
        intermediateStore = intermediateStore.union(intermediateStreamedRdd)
//          .repartition(8)
          .setName(storeName)
        intermediateStore.cache()
       }
    }
  }

  def join(rightRel: DStream[Int],streamLeft: Boolean,joinCondition: ((Int,Int)) => Boolean): DStream[(Int,Int)] = {
    rightRel
      .transform{ streamRdd =>

      if(streamLeft){
        streamRdd.cartesian(storeRdd).filter{ case (a,b) => a < b}

      }else{
        storeRdd.cartesian(streamRdd).filter{ case (a,b) => a < b}
      }
    }
  }

  def intermediateStoreJoin(rightRel: DStream[Int],streamLeft: Boolean, joinCondition: ((Int,Int,Int)) => Boolean): DStream[(Int,Int, Int)] = {
    rightRel.transform{ streamRdd =>
      if(streamLeft){
        streamRdd.cartesian(intermediateStore).map{case (a,(b,c))=> (a,b,c) }.filter{ case (a,b,c) => b< c}

      }else{
        intermediateStore.cartesian(streamRdd).map{case ((a,b),c)=> (a,b,c) }.filter{ case (a,b,c) => b< c}
      }
     }

  }

  def joinWithIntermediateResult(rightRel: DStream[(Int,Int)],streamLeft: Boolean,joinCondition: ((Int,Int,Int)) => Boolean ): DStream[(Int,Int, Int)] = {
    rightRel.transform{ streamRdd =>
      if(streamLeft){
        streamRdd.cartesian(storeRdd).map{case ((a,b),c)=> (a,b,c) }.filter{ case (a,b,c) => b< c}
      }else{
        storeRdd.cartesian(streamRdd).map{case (a,(b,c))=> (a,b,c) }.filter{ case (a,b,c) => b< c}
      }
     }
  }

}
