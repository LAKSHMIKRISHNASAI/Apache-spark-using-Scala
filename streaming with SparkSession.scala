import org.apache.spark.streaming
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Durations.seconds
object streaming {
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder().appName("streaming").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df= spark.readStream.format("socket").option("host","localhost")
      .option("port","4040").load()
    import org.apache.spark.sql.functions.{explode,split,col}
    val wordsdf= df.select(explode(split(col("value")," ")).alias("word"))
    val rdd=wordsdf.groupBy(("word")).count()

    val query= rdd.writeStream.format("console").outputMode("complete")
      .start()
      .awaitTermination()
  }
}
