package dstream

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path


class ThroughputGenericStorage [T] (sc:SparkContext, storeName: String) {
  private var storeRdd: RDD[(T, Long)] = sc.emptyRDD
  var storeSize = 0L

  def store(source: DStream[T], aproximateSize: Long = 0L): DStream[(T, Long)] = {
    source.transform { streamedRdd =>
      var timeRdd = streamedRdd.map((_, System.currentTimeMillis()))
      if (!streamedRdd.isEmpty()) {
        storeRdd = storeRdd.union(timeRdd)
          .setName(storeName)

        storeRdd = storeRdd.cache()
        //        println(s"Storing ${storeRdd.count()} in $storeName")
        timeRdd
      } else {
        sc.emptyRDD
      }
    }
  }

  def join[U](rightRel: DStream[(U, Long)], joinCondition: (((T, Long), (U, Long))) => Boolean, streamSize: Long) = {
    var joinResult = rightRel
      .transform { streamRdd =>
        if (streamRdd.isEmpty() || storeRdd.isEmpty()) {
          var foo: RDD[((T, U), Long, Long)] = sc.emptyRDD
          foo
        }
        else {
          println(s"Joining in $storeName")
          computeJoin(storeRdd, streamRdd, joinCondition, rightBroad = streamSize < storeSize)
        }
      }
    joinResult
  }

  def joinAsRight[U](rightRel: DStream[(U, Long)], joinCondition: (((U, Long), (T, Long))) => Boolean, streamSize: Long) = {
    var joinResult = rightRel
      .transform { streamRdd =>
        if (streamRdd.isEmpty || storeRdd.isEmpty) {
          var foo: RDD[((U, T), Long, Long)] = sc.emptyRDD
          foo
        }
        else {
          println(s"Joining right in $storeName")
          computeJoinAsRight(storeRdd, streamRdd, joinCondition, rightBroad = streamSize < storeSize)
        }
      }
    joinResult
  }

  def computeJoin[U](leftRDD: RDD[(T, Long)], rightRDD: RDD[(U, Long)], joinCondition: (((T, Long), (U, Long))) => Boolean, rightBroad: Boolean) = {

    if (rightBroad) {
      var broadcastData = sc.broadcast(rightRDD.collect())
      val resultRdd = leftRDD.mapPartitions { part =>
        val conf = new Configuration()
        val fs = FileSystem.get(conf)
        var size = part.size * broadcastData.value.length
        conf.set("fs.defaultFS", "hdfs://hdfs-namenode:8020")
        var start = System.nanoTime
        var result = part.flatMap(storedTuple =>
          broadcastData.value.map { broadcastTuple =>
            (storedTuple, broadcastTuple)
          })
        var end = System.nanoTime
        println(s"Tempo = ${end - start}")
        val os = fs.create(new Path("/experiments/throughput/" + end))
        var data = s"$size,${end - start}"
        os.write(data.getBytes)
        result
      }
        .filter { case (a, b) => joinCondition((a.asInstanceOf[(T, Long)], b.asInstanceOf[(U, Long)])) }
        .map { case (a, b) => ((a.asInstanceOf[(T, Long)]._1, b.asInstanceOf[(U, Long)]._1), if (a._2 < b._2) a._2 else b._2, System.currentTimeMillis) }
      broadcastData.unpersist

      resultRdd
    } else {
      var broadcastData = sc.broadcast(leftRDD.collect())
      val resultRdd = rightRDD.mapPartitions { part =>
        val conf = new Configuration()
        val fs = FileSystem.get(conf)
        var size = part.size * broadcastData.value.length
        conf.set("fs.defaultFS", "hdfs://hdfs-namenode:8020")
        var start = System.nanoTime
        var result = part.flatMap(storedTuple =>
          broadcastData.value.map { broadcastTuple =>
            (broadcastTuple, storedTuple)
          })
        var end = System.nanoTime
        println(s"Tempo = ${end - start}")
        val os = fs.create(new Path("/experiments/throughput/" + end))
        var data = s"$size,${end - start}"
        os.write(data.getBytes)
        result
      }
        .filter { case (a, b) => joinCondition((a.asInstanceOf[(T, Long)], b.asInstanceOf[(U, Long)])) }
        .map { case (a, b) => ((a.asInstanceOf[(T, Long)]._1, b.asInstanceOf[(U, Long)]._1), if (a._2 < b._2) a._2 else b._2, System.currentTimeMillis) }
      broadcastData.unpersist

      resultRdd
    }
  }

  def computeJoinAsRight[U](leftRDD: RDD[(T, Long)], rightRDD: RDD[(U, Long)], joinCondition: (((U, Long), (T, Long))) => Boolean, rightBroad: Boolean) = {

    if (rightBroad) {
      var broadcastData = sc.broadcast(rightRDD.collect())
      val resultRdd = leftRDD.mapPartitions { part =>
        val conf = new Configuration()
        val fs = FileSystem.get(conf)
        var size = part.size * broadcastData.value.length
        conf.set("fs.defaultFS", "hdfs://hdfs-namenode:8020")
        var start = System.nanoTime
        var result =
          part.flatMap(storedTuple =>

            broadcastData.value.map { broadcastTuple =>
              (broadcastTuple, storedTuple)
            })
        var end = System.nanoTime
        println(s"Tempo = ${end - start}")
        val os = fs.create(new Path("/experiments/throughput/" + end))
        var data = s"$size,${end - start}"
        os.write(data.getBytes)
        result
      }
        .filter { case (a, b) => joinCondition((a.asInstanceOf[(U, Long)], b.asInstanceOf[(T, Long)])) }
        .map { case (a, b) => ((a.asInstanceOf[(U, Long)]._1, b.asInstanceOf[(T, Long)]._1), if (a._2 < b._2) a._2 else b._2, System.currentTimeMillis) }
      broadcastData.unpersist
      resultRdd
    } else {
      var broadcastData = sc.broadcast(leftRDD.collect())
      val resultRdd = rightRDD.mapPartitions { part =>
        val conf = new Configuration()
        val fs = FileSystem.get(conf)
        var size = part.size * broadcastData.value.length
        conf.set("fs.defaultFS", "hdfs://hdfs-namenode:8020")
        var start = System.nanoTime
        var result =
          part.flatMap(storedTuple =>
            broadcastData.value.map { broadcastTuple =>
              (storedTuple, broadcastTuple)
            })
        var end = System.nanoTime
        println(s"Tempo = ${end - start}")
        val os = fs.create(new Path("/experiments/throughput/" + end))
        var data = s"$size,${end - start}"
        os.write(data.getBytes)
        result
      }
        .filter { case (a, b) => joinCondition((a.asInstanceOf[(U, Long)], b.asInstanceOf[(T, Long)])) }
        .map { case (a, b) => ((a.asInstanceOf[(U, Long)]._1, b.asInstanceOf[(T, Long)]._1), if (a._2 < b._2) a._2 else b._2, System.currentTimeMillis) }
      broadcastData.unpersist
      resultRdd
    }

  }
}