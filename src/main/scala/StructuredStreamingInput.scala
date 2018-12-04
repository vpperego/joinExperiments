
 import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
 import org.apache.spark.sql.catalyst.ScalaReflection
 import org.apache.spark.sql.types.StructType


case class Course(Id: Int)
 case class groupedRows(groupId: Int, row: Int)
object StructuredStreamingInput {

  val spark = SparkSession
    .builder
    .appName("Local App")
    .master("local[*]")
    .getOrCreate
  import spark.implicits._

  val parallelismForA = 3
  val rng = new scala.util.Random

  val relASchema = ScalaReflection.schemaFor[Course].dataType.asInstanceOf[StructType]

  var relA: DataFrame = spark.readStream.schema(relASchema).csv("file:///home/vinicius/IdeaProjects/joinExperiments/resources/relA5K")

  var ds: Dataset[Course] = relA.as[Course]//

  var store = ds.map(c =>   groupedRows(rng.nextInt(parallelismForA),c.Id *2))
}
