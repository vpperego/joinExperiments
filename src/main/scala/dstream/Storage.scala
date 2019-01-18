package dstream

 import org.apache.spark.SparkContext

 import org.apache.spark.rdd.RDD
 import org.apache.spark.streaming.dstream.DStream
 import org.apache.spark.streaming.StreamingContext
class Storage (sc:SparkContext, ssc: StreamingContext, storeName: String) {
  private var storeRdd: RDD[Int] = sc.emptyRDD
   private var intermediateStore: RDD[(Int,Int)] = sc.emptyRDD

   def store (source: DStream[Int]): DStream[Int] = {
     source.transform{ streamedRdd =>
       if(!streamedRdd.isEmpty()){
         println(s"Store $storeName (size ${storeRdd.count}) receiving stream (size ${streamedRdd.count})")
         storeRdd = storeRdd.union(streamedRdd)
           .setName(storeName)
         storeRdd.cache()
         streamedRdd
       }else{
         streamedRdd
       }
       }
  }

  def storeIntermediateResult (source: DStream[(Int,Int)]): DStream[(Int,Int)] = {
    source.transform{ intermediateStreamedRdd =>
      if(!intermediateStreamedRdd.isEmpty){
        println(s"Store $storeName (size ${intermediateStore.count}) receiving stream (size ${intermediateStreamedRdd.count})")
        intermediateStore = intermediateStore.union(intermediateStreamedRdd)
          .setName(storeName)
        intermediateStore.cache()
        intermediateStreamedRdd
      }else{
        intermediateStreamedRdd
      }
     }
  }

  def join(rightRel: DStream[Int],streamLeft: Boolean,joinCondition: ((Int,Int)) => Boolean): DStream[(Int,Int)] = {
    rightRel
      .transform{ streamRdd =>
        if(!streamRdd.isEmpty()) {
          println(s"Join between $storeName (size ${storeRdd.count}) and stream (size ${streamRdd.count})")
        }
        if(streamLeft){
        streamRdd.cartesian(storeRdd).filter{ case (a,b) => a < b}

      }else{
        storeRdd.cartesian(streamRdd).filter{ case (a,b) => a < b}
      }
    }
  }

  def intermediateStoreJoin(rightRel: DStream[Int],streamLeft: Boolean, joinCondition: ((Int,Int,Int)) => Boolean): DStream[(Int,Int, Int)] = {
    rightRel.transform{ streamRdd =>
      if(!streamRdd.isEmpty()) {
        println(s"Join between $storeName (size ${intermediateStore.count}) and stream (size ${streamRdd.count})")
      }

      if(streamLeft){
        streamRdd.cartesian(intermediateStore).map{case (a,(b,c))=> (a,b,c) }.filter{ case (a,b,c) => b< c}

      }else{
        intermediateStore.cartesian(streamRdd).map{case ((a,b),c)=> (a,b,c) }.filter{ case (a,b,c) => b< c}
      }
     }

  }

  def joinWithIntermediateResult(rightRel: DStream[(Int,Int)],streamLeft: Boolean,joinCondition: ((Int,Int,Int)) => Boolean )
  : DStream[(Int,Int,Int)] = {
    rightRel.transform{ streamRdd =>
      if(!streamRdd.isEmpty()) {
        println(s"Join between $storeName (size ${storeRdd.count}) and stream (size ${streamRdd.count})")
      }
      if(streamLeft){
        streamRdd.cartesian(storeRdd).map{case ((a,b),c)=> (a,b,c) }.filter{ case (a,b,c) => b< c}
      }else{
        storeRdd.cartesian(streamRdd).map{case (a,(b,c))=> (a,b,c) }.filter{ case (a,b,c) => b< c}
      }
     }
  }

}
