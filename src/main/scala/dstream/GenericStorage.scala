package dstream

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

class GenericStorage[T] (sc:SparkContext, storeName: String){
  private var storeRdd: RDD[(T, Long)] = sc.emptyRDD

  def store (source: DStream[T]): DStream[(T, Long)] = {
    source.transform{ streamedRdd =>
      var timeRdd  = streamedRdd.map((_,System.currentTimeMillis()))
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

  def join[U](rightRel: DStream[(U, Long)],  joinCondition: (((T,Long),(U,Long))) => Boolean,rightRelation: Boolean=false): DStream[(T,U)] = {
    var joinResult  = rightRel
      .transform { streamRdd =>
        val streamSize = streamRdd.count
        val storeSize = storeRdd.count
        val invert = (rightRelation && storeSize < streamSize) || (!rightRelation && streamSize< storeSize  )
        if(streamRdd.isEmpty()) {
          var foo: RDD[(T, U)]  = sc.emptyRDD
          foo
        }
        else{
          if(streamSize < storeSize){
            computeJoin(storeRdd, streamRdd,invert,joinCondition, rightBroad = true)
          }else{
            computeJoin(storeRdd,streamRdd,invert,joinCondition, rightBroad = false)
          }
        }
      }
    joinResult
  }


  def joinAsRight[U](rightRel: DStream[(U, Long)],  joinCondition: (((U,Long),(T,Long))) => Boolean,rightRelation: Boolean=false): DStream[(U,T)] = {
    var joinResult  = rightRel
      .transform { streamRdd =>
        val streamSize = streamRdd.count
        val storeSize = storeRdd.count
        val invert = (rightRelation && storeSize < streamSize) || (!rightRelation && streamSize< storeSize  )
        if(streamRdd.isEmpty()) {
          var foo: RDD[(U, T)]  = sc.emptyRDD
          foo
        }
        else{
          println("Joining in $storeName")
          if(streamSize < storeSize){
            computeJoinAsRight(storeRdd,streamRdd,invert,joinCondition,rightBroad = true)
          }else{
            computeJoinAsRight(storeRdd,streamRdd,invert,joinCondition,rightBroad = false)
          }

        }
      }
    joinResult
  }

  def computeJoin[U](leftRDD: RDD[(T, Long)], rightRDD: RDD[(U, Long)], invert: Boolean, joinCondition: (((T,Long),(U,Long))) => Boolean,rightBroad: Boolean)  = {

    if(rightBroad){
      var broadcastData = sc.broadcast(rightRDD.collect())
      val resultRdd: RDD[(T, U)] = leftRDD.mapPartitions{ part =>
        if(invert){
          part.flatMap(storedTuple =>
            broadcastData.value.map{ broadcastTuple =>
              (broadcastTuple, storedTuple)
            })
        }
        else{
          part.flatMap(storedTuple =>
            broadcastData.value.map{ broadcastTuple =>
              (storedTuple, broadcastTuple)
            })
        }
      }
      .filter{case (a,b) => joinCondition((a.asInstanceOf[(T,Long)],b.asInstanceOf[(U,Long)]))}
      .map{case (a,b) => (a.asInstanceOf[(T,Long)]._1, b.asInstanceOf[(U,Long)]._1)}
      broadcastData.unpersist

      resultRdd

    }else{
      var broadcastData  =  sc.broadcast(leftRDD.collect())

      val resultRdd: RDD[(T, U)] = leftRDD.mapPartitions{ part =>
        if(invert){
          part.flatMap(storedTuple =>
            broadcastData.value.map{ broadcastTuple =>
              (broadcastTuple, storedTuple)
            })
        }
        else{
          part.flatMap(storedTuple =>
            broadcastData.value.map{ broadcastTuple =>
              (storedTuple, broadcastTuple)
            })
        }
      }
        .filter{case (a,b) => joinCondition((a.asInstanceOf[(T,Long)],b.asInstanceOf[(U,Long)]))}
        .map{case (a,b) => (a.asInstanceOf[(T,Long)]._1, b.asInstanceOf[(U,Long)]._1)}
      broadcastData.unpersist

      resultRdd
    }
  }

  def computeJoinAsRight[U](leftRDD: RDD[(T, Long)], rightRDD: RDD[(U, Long)], invert: Boolean, joinCondition: (((U,Long),(T,Long))) => Boolean,rightBroad: Boolean)  = {

    var broadcastData  = if (rightBroad) sc.broadcast(rightRDD.collect()) else sc.broadcast(leftRDD.collect())

    if(rightBroad){
      var broadcastData  = sc.broadcast(rightRDD.collect())
      val resultRdd: RDD[(U, T)] = leftRDD.mapPartitions{ part =>
        if(invert){
          part.flatMap(storedTuple =>
            broadcastData.value.map{ broadcastTuple =>
              (broadcastTuple, storedTuple)
            })
        }
        else{
          part.flatMap(storedTuple =>
            broadcastData.value.map{ broadcastTuple =>
              (storedTuple, broadcastTuple)
            })
        }
      }
        .filter{case (a,b) => joinCondition((a.asInstanceOf[(U,Long)],b.asInstanceOf[(T,Long)]))}
        .map{case (a,b) => (b.asInstanceOf[(U,Long)]._1, a.asInstanceOf[(T,Long)]._1)}
      broadcastData.unpersist
      resultRdd
    }else{
      var broadcastData  =  sc.broadcast(leftRDD.collect())
      val resultRdd: RDD[(U, T)] = leftRDD.mapPartitions{ part =>
        if(invert){
          part.flatMap(storedTuple =>
            broadcastData.value.map{ broadcastTuple =>
              (broadcastTuple, storedTuple)
            })
        }
        else{
          part.flatMap(storedTuple =>
            broadcastData.value.map{ broadcastTuple =>
              (storedTuple, broadcastTuple)
            })
        }
      }
        .filter{case (a,b) => joinCondition((a.asInstanceOf[(U,Long)],b.asInstanceOf[(T,Long)]))}
        .map{case (a,b) => (b.asInstanceOf[(U,Long)]._1, a.asInstanceOf[(T,Long)]._1)}
      broadcastData.unpersist
      resultRdd
    }

  }
}
