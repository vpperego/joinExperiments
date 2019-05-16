package dstream

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

class GenericStorage[T] (sc:SparkContext, storeName: String){
  private var storeRdd: RDD[(T, Long)] = sc.emptyRDD
	 var storeSize = 0L
  def store (source: DStream[T],aproximateSize:Long =0L): DStream[(T, Long)] = {
    source.transform{ streamedRdd =>
      var timeRdd  = streamedRdd.map((_,System.currentTimeMillis()))
      if(!streamedRdd.isEmpty()){
        storeRdd = storeRdd.union(timeRdd)
          .setName(storeName)

        storeRdd = storeRdd.cache()
        timeRdd
      }else{
        sc.emptyRDD
      }
    }
  }

  def join[U](rightRel: DStream[(U, Long)],  joinCondition: (((T,Long),(U,Long))) => Boolean) = {
    var joinResult  = rightRel
      .transform { streamRdd =>
        if(streamRdd.isEmpty() || storeRdd.isEmpty()) {
          var foo: RDD[((T, U),Long)] = sc.emptyRDD
          foo
        }
        else{
            println(s"Joining in $storeName")
           computeJoin(storeRdd, streamRdd,joinCondition)
        }
      }
    joinResult
  }

  def joinAsRight[U](rightRel: DStream[(U, Long)],  joinCondition: (((U,Long),(T,Long))) => Boolean) = {
    var joinResult  = rightRel
      .transform { streamRdd =>
        if(streamRdd.isEmpty || storeRdd.isEmpty) {
          var foo: RDD[((U, T),Long)]  = sc.emptyRDD
          foo
        }
        else{
          println(s"Joining right in $storeName")
          computeJoinAsRight(storeRdd,streamRdd,joinCondition)
        }
      }
    joinResult
  }

  def joinFinal[U](rightRel: DStream[(U, Long)],  joinCondition: (((T,Long),(U,Long))) => Boolean) = {
    var joinResult  = rightRel
      .transform { streamRdd =>
        if(streamRdd.isEmpty() || storeRdd.isEmpty()) {
          var foo: RDD[((T, U),Long,Long)] = sc.emptyRDD
          foo
        }
        else{
          println(s"Joining in $storeName")
          computeJoinFinal(storeRdd, streamRdd,joinCondition)
        }
      }
    joinResult
  }

  def joinAsRightFinal[U](rightRel: DStream[(U, Long)],  joinCondition: (((U,Long),(T,Long))) => Boolean) = {
    var joinResult  = rightRel
      .transform { streamRdd =>
        if(streamRdd.isEmpty || storeRdd.isEmpty) {
          var foo: RDD[((U, T),Long,Long)] = sc.emptyRDD
          foo
        }
        else{
          println(s"Joining right in $storeName")
          computeJoinAsRightFinal(storeRdd,streamRdd,joinCondition)
        }
      }
    joinResult
  }

  def computeJoin[U](leftRDD: RDD[(T, Long)], rightRDD: RDD[(U, Long)], joinCondition: (((T,Long),(U,Long))) => Boolean)  = {
      var broadcastData = sc.broadcast(rightRDD.collect())
      val resultRdd  = leftRDD.mapPartitions{ part =>
          part.flatMap(storedTuple =>
            broadcastData.value.map{ broadcastTuple =>
              (storedTuple, broadcastTuple)
            })
      }
      .filter{case (a,b) => joinCondition((a.asInstanceOf[(T,Long)],b.asInstanceOf[(U,Long)]))}
      .map{case (a,b) => ((a.asInstanceOf[(T,Long)]._1, b.asInstanceOf[(U,Long)]._1),if(a._2<b._2) a._2 else b._2)}
      broadcastData.unpersist

      resultRdd

  }

  def computeJoinAsRight[U](leftRDD: RDD[(T, Long)], rightRDD: RDD[(U, Long)], joinCondition: (((U,Long),(T,Long))) => Boolean)  = {

      var broadcastData  = sc.broadcast(rightRDD.collect())
      val resultRdd = leftRDD.mapPartitions{ part =>
          part.flatMap(storedTuple =>
            broadcastData.value.map{ broadcastTuple =>
              (broadcastTuple, storedTuple)
            })
      }
        .filter{case (a,b) => joinCondition((a.asInstanceOf[(U,Long)],b.asInstanceOf[(T,Long)]))}
        .map{case (a,b) => ((a.asInstanceOf[(U,Long)]._1, b.asInstanceOf[(T,Long)]._1),if(a._2<b._2) a._2 else b._2)}
      broadcastData.unpersist
      resultRdd
  }

  def computeJoinFinal[U](leftRDD: RDD[(T, Long)], rightRDD: RDD[(U, Long)], joinCondition: (((T,Long),(U,Long))) => Boolean)  = {

      var broadcastData = sc.broadcast(rightRDD.collect())
      val resultRdd  = leftRDD.mapPartitions{ part =>
        part.flatMap(storedTuple =>
          broadcastData.value.map{ broadcastTuple =>
            (storedTuple, broadcastTuple)
          })
      }
        .filter{case (a,b) => joinCondition((a.asInstanceOf[(T,Long)],b.asInstanceOf[(U,Long)]))}
        .map{case (a,b) => ((a.asInstanceOf[(T,Long)]._1, b.asInstanceOf[(U,Long)]._1),if(a._2<b._2) a._2 else b._2,System.currentTimeMillis())}
      broadcastData.unpersist

      resultRdd
  }

  def computeJoinAsRightFinal[U](leftRDD: RDD[(T, Long)], rightRDD: RDD[(U, Long)], joinCondition: (((U,Long),(T,Long))) => Boolean)  = {

      var broadcastData  = sc.broadcast(rightRDD.collect())
      val resultRdd = leftRDD.mapPartitions{ part =>
        part.flatMap(storedTuple =>
          broadcastData.value.map{ broadcastTuple =>
            (broadcastTuple, storedTuple)
          })
      }
        .filter{case (a,b) => joinCondition((a.asInstanceOf[(U,Long)],b.asInstanceOf[(T,Long)]))}
        .map{case (a,b) => ((a.asInstanceOf[(U,Long)]._1, b.asInstanceOf[(T,Long)]._1),if(a._2<b._2) a._2 else b._2,System.currentTimeMillis())}
      broadcastData.unpersist
      resultRdd
     }
}