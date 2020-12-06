package CartesianJoin

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import util.DStreamUtils

class KafkaTwoRelationsJoin {
  val sc = SparkSession.getActiveSession.get.sparkContext
  val ssc = new StreamingContext(sc, Seconds(4))
  var utils = new DStreamUtils

  val stream_R  = utils.createKafkaStream(ssc, Array("stream_r"), "stream_r").map(row => (row.toInt, System.currentTimeMillis()))
  val stream_S = utils.createKafkaStream(ssc, Array("stream_s"), "stream_s").map(row => (row.toInt, System.currentTimeMillis()))

  var storage_R = new ThetaJoinStorage[(Int, Long)](sc)
  var storage_S = new ThetaJoinStorage[(Int, Long)](sc)

  var probed_R = storage_R.store(stream_R)
  var probed_S = storage_S.store(stream_S)

  val join_predicate_R = (left_row:(Int, Long), right_row: (Int, Long)) => left_row._1 == right_row._1
  val join_predicate_S = (right_row:(Int, Long), left_row: (Int, Long)) => left_row._1 == right_row._1


  val result_1 = storage_R.join(probed_S, join_predicate_R)
  val result_2 = storage_S.joinAsRight(probed_R, join_predicate_S)

  result_1.union(result_2).print()

  ssc.start()
  ssc.awaitTerminationOrTimeout(Minutes(2).milliseconds)
}
