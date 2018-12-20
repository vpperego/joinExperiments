package structuredStreaming


import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class Storage (spark: SparkSession, storageName: String){
  import spark.implicits._

  var currentBatch: Long= -1 ;
  private var storeDataset:Dataset[Int]  = spark.createDataset(spark.sparkContext.emptyRDD[Int])

  def store (source: Dataset[Int],refreshTime: Long): Unit = {
    source
      .writeStream
      .foreachBatch{ (batchDF: Dataset[Int], batchId: Long) =>
        var updateStore = false;
        if(batchId==0){
          currentBatch = 0;
          updateStore = true
        }
        else if(batchId > currentBatch) {
          currentBatch = currentBatch +1;
          updateStore = true
        }
        if(updateStore){
          storeDataset = storeDataset.union(batchDF)
          storeDataset .cache
          println(s"Stored elements in $storageName : ${storeDataset.count} . batchId: $batchId - currentBatch: $currentBatch ")
        }
      }.start
      .awaitTermination(refreshTime)
  }
  def join(other: Dataset[Int]): Dataset[(Int,Int)] = {
      other.transform{newData =>
        newData.crossJoin(storeDataset).as[(Int,Int)]}
      }
}
