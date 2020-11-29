package BroadcastJoin

import CartesianJoin.ThetaJoinStorage
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

//case class Tuple(key: Int, timestamp: Long) TODO - define tuple class for join predicates
class TwoRelations {

  val sc = SparkSession.getActiveSession.get.sparkContext
  val ssc = new StreamingContext(sc, Seconds(12))

  val stream_R: DStream[(Int, Long)] = ssc.socketTextStream("localhost", 9990).map(row => (row.toInt, System.currentTimeMillis()))

  val stream_S = ssc.socketTextStream("localhost", 9991).map(row => (row.toInt, System.currentTimeMillis()))
  var storage_R = new ThetaJoinStorage[(Int, Long)](sc)
  var storage_S = new ThetaJoinStorage[(Int, Long)](sc)

  var probed_R = storage_R.store(stream_R)
  var probed_S = storage_S.store(stream_S)

  val join_predicate_R = (left_row:(Int, Long), right_row: (Int, Long)) => left_row._1 == right_row._1 && left_row._2 < right_row._2
  val join_predicate_S = (left_row:(Int, Long), right_row: (Int, Long)) => left_row._1 == right_row._1 && right_row._2 < left_row._2

  val result_1 = storage_R.join(probed_S, join_predicate_R)
  val result_2 = storage_S.join(probed_R, join_predicate_S)
//  val result_1 = storage_R.cartesianJoin(probed_S)
//  val result_2 = storage_S.cartesianJoin(probed_R)
  result_1.union(result_2).print()

  ssc.start()
  ssc.awaitTerminationOrTimeout(Minutes(1).milliseconds)

}
