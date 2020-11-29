package BroadcastJoin

 import org.apache.spark.SparkContext

 import org.apache.spark.rdd.RDD
 import org.apache.spark.streaming.dstream.DStream

class Storage (sc:SparkContext, storeName: String,rightRelation: Boolean = false) {
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
        if(rightRelation){
          streamRdd.cartesian(storeRdd).filter(joinCondition)
        }else{
          storeRdd.cartesian(streamRdd).filter(joinCondition)
        }
      }.map(row => (row._1._1, row._2._1))
  }
}
