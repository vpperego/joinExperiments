package dstream

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable.ArrayBuffer

class NewStorage (sc:SparkContext, ssc: StreamingContext, storeName: String) {
  private var storeRdd: RDD[Int] = sc.emptyRDD

  def store (source: DStream[Int]): DStream[Int] = {
    source.transform{ streamedRdd =>
      if(!streamedRdd.isEmpty()){
        streamedRdd.map((_,System.currentTimeMillis()))
        storeRdd = storeRdd.union(streamedRdd)
          .setName(storeName)
        storeRdd.cache()
        streamedRdd
      }else{
        streamedRdd
      }
    }
  }


  def join(rightRel: DStream[Int], rightRelStream: Boolean, joinCondition: ((Int,Int)) => Boolean): DStream[(Int,Int)] = {
     rightRel
      .transform{ streamRdd =>
        val streamSize = streamRdd.count
        val storeSize = storeRdd.count
        if(streamRdd.isEmpty()) {
          streamRdd.cartesian(storeRdd).filter{ case (a,b) => a < b}
        }else{
          if(streamSize < storeSize){
            anotherComputeJoin(streamRdd,storeRdd,rightRelStream)
          }else{
            anotherComputeJoin(storeRdd,streamRdd,rightRelStream)
          }
        }
      }
  }
    def computeJoin(normalRdd: RDD[Int], broadRdd: RDD[Int], rightRelStream: Boolean): RDD[(Int, Int)] = {
      var broadStorage: Broadcast[Array[Int]] = sc.broadcast(broadRdd.collect())

      val resultRdd: RDD[(Int, Int)] = normalRdd.mapPartitions{ part =>
        var foo = new ArrayBuffer[(Int,Int)]
         part.foreach{ streamTuple =>
          broadStorage.value.foreach{ storedTuple =>
            if(rightRelStream){
              if(storedTuple < streamTuple){
                foo.append((storedTuple,streamTuple))
              }
            }else{
              if(streamTuple< storedTuple ){
                foo.append((streamTuple,storedTuple))
              }
            }
          }
        }
        foo.toIterator
       }
      broadStorage.unpersist
      resultRdd
  }

  def anotherComputeJoin(normalRdd: RDD[Int], broadRdd: RDD[Int], rightRelStream: Boolean): RDD[(Int, Int)] = {
    var broadStorage: Broadcast[Array[Int]] = sc.broadcast(broadRdd.collect())
    val resultRdd: RDD[(Int, Int)] = normalRdd.mapPartitions{ part =>

          if(rightRelStream){
            part.flatMap(streamTuple =>
              broadStorage.value.map{ storedTuple =>
                (storedTuple, streamTuple)
              }).filter{ case (a,b) => a < b}
          }else{
            part.flatMap(streamTuple =>
              broadStorage.value.map{ storedTuple =>
                (streamTuple, storedTuple)
              }).filter{ case (a,b) => a < b}
          }
     }
    broadStorage.unpersist
    resultRdd
  }
}
