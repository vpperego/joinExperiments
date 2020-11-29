package BroadcastJoin

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

class NewStorage (sc:SparkContext, storeName: String, rightRelation: Boolean = false) {
  private var storeRdd: RDD[(Int, Long)] = sc.emptyRDD

  def store (source: DStream[Int]): DStream[(Int, Long)] = {
    source.transform{ streamedRdd =>
      var timeRdd: RDD[(Int, Long)] = streamedRdd.map((_,System.currentTimeMillis()))
      if(!streamedRdd.isEmpty()){
        println(s"Storing ${streamedRdd.count()} in $storeName")

        storeRdd = storeRdd.union(timeRdd)
          .setName(storeName)

        storeRdd.cache()
        timeRdd
      }else{
        timeRdd
      }
    }
  }


  def join(rightRel: DStream[(Int, Long)], joinCondition: (((Int, Long),(Int, Long))) => Boolean): DStream[(Int, Int)] = {
     rightRel
      .transform{ streamRdd =>
        val streamSize = streamRdd.count
        val storeSize = storeRdd.count
        if(streamSize <= 0) {
          streamRdd.map(row => (row._1,row._1))
        }
        else{
           val invert = (rightRelation && streamSize < storeSize) || (!rightRelation &&  storeSize  < streamSize)
          if(streamSize < storeSize){
            println(s"Broadcasting stream.Invert is $invert in $storeName")
            computeJoin(storeRdd,streamRdd,invert,joinCondition)
          }else{
            println(s"Broadcasting store.Invert is $invert in $storeName")

            computeJoin(streamRdd,storeRdd,invert,joinCondition)
          }
        }
      }
  }


  def joinWithIntermediateResult(rightRel: DStream[((Int,Int), Long)],joinCondition: ((((Int,Int), Long),(Int, Long))) => Boolean): DStream[(Int, Int, Int)] = {
    rightRel
      .transform{ streamRdd =>
        val streamSize = streamRdd.count
        val storeSize = storeRdd.count
        val invert = (rightRelation && streamSize < storeSize) || (!rightRelation &&  storeSize  < streamSize)

        if(streamRdd.isEmpty()) {
          streamRdd.map(row => (row._1._1,row._1._1, row._1._1))
        }else{
          val streamSize = streamRdd.count
          val storeSize = storeRdd.count
          val invert = (rightRelation && streamSize < storeSize) || (!rightRelation &&  storeSize  < streamSize)
          if(streamSize < storeSize){
            intermediateBroadcastJoin(streamRdd,storeRdd, joinCondition)
          }else{
            relationBroadcastJoin(storeRdd,streamRdd, joinCondition)
          }        }

      }
  }

  def intermediateBroadcastJoin(normalRdd: RDD[((Int, Int), Long)], broadRdd: RDD[(Int, Long)],joinCondition: ((((Int,Int), Long),(Int, Long))) => Boolean): RDD[(Int, Int, Int)] = {
    var broadcastData: Broadcast[Array[(Int, Long)]] = sc.broadcast(broadRdd.collect())

    val resultRdd  = normalRdd.mapPartitions{ part =>
      var bar: Iterator[(((Int, Int), Long), (Int, Long))] = part.flatMap(storedTuple =>
        broadcastData.value.map{ broadcastTuple =>
          (storedTuple, broadcastTuple)
        })
      bar.filter{joinCondition}
        .map(row => (row._1._1._1, row._1._1._2, row._2._1))
    }
    broadcastData.unpersist
    resultRdd
  }

  def relationBroadcastJoin(normalRdd: RDD[(Int, Long)], broadRdd: RDD[((Int, Int), Long)],joinCondition: ((((Int,Int), Long),(Int, Long))) => Boolean): RDD[(Int, Int, Int)] = {
    var broadcastData  = sc.broadcast(broadRdd.collect())

    val resultRdd  = normalRdd.mapPartitions{ part =>
      var bar: Iterator[(((Int, Int), Long), (Int, Long))] = part.flatMap(storedTuple =>
        broadcastData.value.map{ broadcastTuple =>
          (broadcastTuple, storedTuple)
        })
      bar.filter{joinCondition}
        .map(row => (row._1._1._1, row._1._1._2, row._2._1))
    }
    broadcastData.unpersist
    resultRdd
  }

  def computeJoin(normalRdd: RDD[(Int, Long)], broadRdd: RDD[(Int, Long)], invert: Boolean,joinCondition: (((Int, Long),(Int, Long))) => Boolean): RDD[(Int, Int)] = {

    var broadcastedData: Broadcast[Array[(Int, Long)]] = sc.broadcast(broadRdd.collect())

    val resultRdd  = normalRdd.mapPartitions{ part =>
      if(invert){
        part.flatMap(storedTuple =>
          broadcastedData.value.map{ broadcastTuple =>
            (broadcastTuple, storedTuple)
          })
      }
      else{
        part.flatMap(storedTuple =>
          broadcastedData.value.map{ broadcastTuple =>
            (storedTuple, broadcastTuple)
          })
      }
    }.filter(joinCondition)
      .map(row => (row._1._1, row._2._1))
    broadcastedData.unpersist

    resultRdd
  }


}
