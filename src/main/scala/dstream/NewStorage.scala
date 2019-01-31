package dstream

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext

class NewStorage (sc:SparkContext, ssc: StreamingContext, storeName: String) {
  private var storeRdd: RDD[(Int, Long)] = sc.emptyRDD

  def store (source: DStream[Int]): DStream[(Int, Long)] = {
    source.transform{ streamedRdd =>
      var timeRdd: RDD[(Int, Long)] = streamedRdd.map((_,System.currentTimeMillis()))
      if(!streamedRdd.isEmpty()){
        println(s"Storing ${streamedRdd.count()} in $storeName")
        var oldStore = storeRdd

        storeRdd = storeRdd.union(timeRdd)
          .setName(storeName)
        oldStore.unpersist()// TODO - this may mess things up

        storeRdd.cache()
        timeRdd
      }else{
        timeRdd
      }
    }
  }


  def join(rightRel: DStream[(Int, Long)], rightRelStream: Boolean, joinCondition: ((Int,Int)) => Boolean): DStream[(Int, Int)] = {
     rightRel
      .transform{ streamRdd =>
        val streamSize = streamRdd.count
        val storeSize = storeRdd.count
        if(streamRdd.isEmpty()) {
          streamRdd.map(row => (row._1,row._1))
        }
        else{
//          if(streamSize < storeSize){
//            anotherComputeJoin(streamRdd,storeRdd,rightRelStream) //TODO: you know what to do
//          }else{
            anotherComputeJoin(storeRdd,streamRdd,rightRelStream)
//          }
        }
      }
  }


  def joinWithIntermediateResult(rightRel: DStream[((Int,Int), Long)]): DStream[(Int, Int, Int)] = {
    rightRel
      .transform{ streamRdd =>
        val streamSize = streamRdd.count
        val storeSize = storeRdd.count
        if(streamRdd.isEmpty()) {
          streamRdd.map(row => (row._1._1,row._1._1, row._1._1))
        }else{
          anotherComputeJoin2(storeRdd,streamRdd)
        }

      }
  }

  def anotherComputeJoin2(normalRdd: RDD[(Int, Long)], broadRdd: RDD[((Int, Int), Long)] ): RDD[(Int, Int, Int)] = {
    var broadcastedData = sc.broadcast(broadRdd.collect())

    val resultRdd  = normalRdd.mapPartitions { part =>
      var bar: Iterator[(((Int, Int), Long), (Int, Long))] = part.flatMap(storedTuple =>
        broadcastedData.value.map{ streamTuple =>
          (streamTuple,storedTuple)
        })
      bar
        .filter{case (a,b) => a._1._2 <  b._1  && b._2 < a._2 }
        .map(row => (row._1._1._1, row._1._1._2, row._2._1))
    }
    broadcastedData.unpersist
    resultRdd
  }

  def anotherComputeJoin(normalRdd: RDD[(Int, Long)], broadRdd: RDD[(Int, Long)], rightRelStream: Boolean): RDD[(Int, Int)] = {

    var broadcastedData: Broadcast[Array[(Int, Long)]] = sc.broadcast(broadRdd.collect())

    val resultRdd  = normalRdd.mapPartitions{ part =>
        if(rightRelStream){
         part.flatMap(storedTuple =>
            broadcastedData.value.map{ streamTuple =>
              (storedTuple, streamTuple)
            }).filter{ case (a,b) => a._1 < b._1 && a._2 < b._2}.map(row => (row._1._1, row._2._1))
        }
        else{
          part.flatMap(storedTuple =>
            broadcastedData.value.map{ streamTuple =>
              (streamTuple, storedTuple)
          })
            .filter{ case (a,b) => a._1 < b._1 && b._2 < a._2}
            .map(row => (row._1._1, row._2._1))
        }
     }
    broadcastedData.unpersist
    resultRdd
  }

//  def computeJoin(normalRdd: RDD[Int], broadRdd: RDD[Int], rightRelStream: Boolean): RDD[(Int, Int)] = {
//    var broadStorage: Broadcast[Array[Int]] = sc.broadcast(broadRdd.collect())
//
//    val resultRdd: RDD[(Int, Int)] = normalRdd.mapPartitions{ part =>
//      var foo = new ArrayBuffer[(Int,Int)]
//      part.foreach{ streamTuple =>
//        broadStorage.value.foreach{ storedTuple =>
//          if(rightRelStream){
//            if(storedTuple < streamTuple){
//              foo.append((storedTuple,streamTuple))
//            }
//          }else{
//            if(streamTuple< storedTuple ){
//              foo.append((streamTuple,storedTuple))
//            }
//          }
//        }
//      }
//      foo.toIterator
//    }
//    broadStorage.unpersist
//    resultRdd
//  }



}
