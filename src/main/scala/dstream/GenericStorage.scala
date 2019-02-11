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
        println(s"${streamedRdd.count()}  stored in $storeName")

        timeRdd
      }else{
        sc.emptyRDD
      }
    }
  }

  def join[U](rightRel: DStream[(U, Long)],  joinCondition: (((T,Long),(U,Long))) => Boolean,streamSize: Long): DStream[(T,U)] = {
    var joinResult  = rightRel
      .transform { streamRdd =>
        if(streamRdd.isEmpty() || storeRdd.isEmpty()) {
          var foo: RDD[(T, U)]  = sc.emptyRDD
          foo
        }
        else{
//          val streamSize = streamRdd.count
            println(s"Joining in $storeName")
           computeJoin(storeRdd, streamRdd,joinCondition, rightBroad = streamSize < storeSize)
        }
      }
    joinResult
  }

  def joinAsRight[U](rightRel: DStream[(U, Long)],  joinCondition: (((U,Long),(T,Long))) => Boolean,streamSize: Long): DStream[(U,T)] = {
    var joinResult  = rightRel
      .transform { streamRdd =>
        if(streamRdd.isEmpty || storeRdd.isEmpty) {
          var foo: RDD[(U, T)]  = sc.emptyRDD
          foo
        }
        else{
          println(s"Joining right in $storeName")
          computeJoinAsRight(storeRdd,streamRdd,joinCondition,rightBroad = streamSize < storeSize)
        }
      }
    joinResult
  }

  def computeJoin[U](leftRDD: RDD[(T, Long)], rightRDD: RDD[(U, Long)], joinCondition: (((T,Long),(U,Long))) => Boolean,rightBroad: Boolean)  = {

    if(rightBroad){
      var broadcastData = sc.broadcast(rightRDD.collect())
      val resultRdd: RDD[(T, U)] = leftRDD.repartition(4).mapPartitions{ part =>
          part.flatMap(storedTuple =>
            broadcastData.value.map{ broadcastTuple =>
              (storedTuple, broadcastTuple)
            })
      }
      .filter{case (a,b) => joinCondition((a.asInstanceOf[(T,Long)],b.asInstanceOf[(U,Long)]))}
      .map{case (a,b) => (a.asInstanceOf[(T,Long)]._1, b.asInstanceOf[(U,Long)]._1)}
      broadcastData.unpersist

      resultRdd
        //.cache()

    }else{
      var broadcastData  =  sc.broadcast(leftRDD.collect())

      val resultRdd: RDD[(T, U)] = rightRDD.repartition(4).mapPartitions{ part =>
          part.flatMap(storedTuple =>
            broadcastData.value.map{ broadcastTuple =>
              (broadcastTuple,storedTuple )
            })
       }
        .filter{case (a,b) => joinCondition((a.asInstanceOf[(T,Long)],b.asInstanceOf[(U,Long)]))}
        .map{case (a,b) => (a.asInstanceOf[(T,Long)]._1, b.asInstanceOf[(U,Long)]._1)}
      broadcastData.unpersist

      resultRdd
        //.cache()
    }
  }

  def computeJoinAsRight[U](leftRDD: RDD[(T, Long)], rightRDD: RDD[(U, Long)], joinCondition: (((U,Long),(T,Long))) => Boolean,rightBroad: Boolean)  = {

    if(rightBroad){
      var broadcastData  = sc.broadcast(rightRDD.collect())
      val resultRdd = leftRDD.repartition(4).mapPartitions{ part =>
          part.flatMap(storedTuple =>
            broadcastData.value.map{ broadcastTuple =>
              (broadcastTuple, storedTuple)
            })
      }
        .filter{case (a,b) => joinCondition((a.asInstanceOf[(U,Long)],b.asInstanceOf[(T,Long)]))}
        .map{case (a,b) => (a.asInstanceOf[(U,Long)]._1, b.asInstanceOf[(T,Long)]._1)}
      broadcastData.unpersist
      resultRdd
        //.cache()
    }else{
      var broadcastData  =  sc.broadcast(leftRDD.collect())
      val resultRdd  = rightRDD.repartition(4).mapPartitions{ part =>
          part.flatMap(storedTuple =>
            broadcastData.value.map{ broadcastTuple =>
              (storedTuple, broadcastTuple)
            })
      }
        .filter{case (a,b) => joinCondition((a.asInstanceOf[(U,Long)],b.asInstanceOf[(T,Long)]))}
        .map{case (a,b) => (a.asInstanceOf[(U,Long)]._1, b.asInstanceOf[(T,Long)]._1)}
      broadcastData.unpersist
      resultRdd
        //.cache()
    }

  }
}