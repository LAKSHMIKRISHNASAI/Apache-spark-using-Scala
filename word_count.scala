// import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
// import org.apache.spark.rdd.RDD
// import org.apache.spark.sql.Row
object hello {
  def main(args:Array[String]): Unit = {

   println("finally running scala")
   val conf= new SparkConf().setAppName("myfirstapp").setMaster("local[2]");
   val sc= new SparkContext(conf);
   val file= sc.textFile("D://new_downloads//scala//spark_demo_app//src//main//scala//flight_price_prediction.csv").cache();
   val word_count=file.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
//      reduceByKey((x,y)=>(x+y));
   word_count.collect().foreach(println)
//    word_count.saveAsTextFile("D://new_downloads//scala//spark_demo_app"+java.util.UUID.randomUUID.toString);




   














  }

}
