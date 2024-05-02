import org.apache.spark.streaming._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Durations.seconds
//import org.apache.spark.streaming
import java.io.File
object streaming {
  def main(args:Array[String]):Unit={
//    val conf= new SparkConf().setAppName("streaming Application").setMaster("local[2]")
//    val sc= new SparkContext(conf)
//    val ssc= new StreamingContext(sc,seconds(10))
//    val streamRDD= ssc.socketTextStream("localhost",4040)
//
////    val textfile= sc.textFile("D://new_downloads//scala//spark_demo_app//src//main//scala//abc.txt")
//    val count=streamRDD.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey((x,y)=>(x+y))
//    count.print()
//    ssc.start()
//    ssc.awaitTermination()

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
