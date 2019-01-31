package dstream

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

class NewStorageIntermediate (sc:SparkContext, ssc: StreamingContext, storeName: String){
  private var storeRdd: RDD[((Int, Int), Long)] = sc.emptyRDD
  storeRdd = storeRdd.setName(storeName)

  def store(source: DStream[(Int,Int)]): DStream[((Int, Int), Long)] = {
   source.transform{ streamedRdd =>
      var timeRdd = streamedRdd.map((_,System.currentTimeMillis()))
      if(!streamedRdd.isEmpty()){
        println(s"Storing ${streamedRdd.count} in $storeName")
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

  def join(rightRel: DStream[(Int, Long)]): DStream[(Int, Int, Int)]= {
    rightRel
      .transform{ streamRdd =>
        val streamSize = streamRdd.count
        val storeSize = storeRdd.count
        if(streamRdd.isEmpty()) {
          streamRdd.map(row => (row._1,row._1, row._1))
        }
        else{
         anotherComputeJoin(storeRdd,streamRdd)
        }
      }
  }

  def anotherComputeJoin(normalRdd: RDD[((Int, Int), Long)], broadRdd: RDD[(Int, Long)]): RDD[(Int, Int, Int)] = {
    var broadcastedData: Broadcast[Array[(Int, Long)]] = sc.broadcast(broadRdd.collect())

    val resultRdd  = normalRdd.mapPartitions{ part =>
        var bar: Iterator[(((Int, Int), Long), (Int, Long))] = part.flatMap(storedTuple =>
          broadcastedData.value.map{ streamTuple =>
            (storedTuple, streamTuple)
          })
        bar.filter{case (a,b) =>  a._1._2 < b._1 && a._2 < b._2 }
          .map(row => (row._1._1._1, row._1._1._2, row._2._1))
    }
    broadcastedData.unpersist
    resultRdd
  }

}
