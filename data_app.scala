//package src.main.scala
//package myapp
import org.apache.commons.collections.CollectionUtils.select
import org.apache.hadoop.shaded.org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.functions.{concat, current_timestamp, expr, length, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{expr,current_timestamp}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
object data_app {
  def main(args: Array[String]): Unit = {
    val spark= SparkSession.builder().appName("new app").master("local[*]").config("spark.driver.bindAddress","127.0.0.1").getOrCreate();
//    val df: DataFrame= spark.read.option("header",value=true).option("inferSchema",value=true).csv("D://new_downloads//scala//spark_demo_app//src//main//scala//spam.csv");
//    val header=df.first()
//    spark.sparkContext.setLogLevel("ERROR")
//    val rdd= spark.sparkContext.parallelize(List(1,3,5,7,8,9,10,13,15,17,19))
//    val minimum= rdd.reduce((x,y)=>x min y);
//    val maximum = rdd.reduce((x,y)=>x max y)
//    val sum= rdd.reduce((x,y)=>x+y);
//    import org.apache.spark.sql.functions.length
////    println(rdd.reduce((x,y)=> (x+y)/length))
//    println(minimum);
//    println(maximum);
//    println(sum)
//    val rdd2= spark.sparkContext.parallelize(List("Germany India USA","USA London Russia","Mexico Brazil Canada China"))
//    val word=rdd2.flatMap(_.split(" "));
//    val sortRDD=word.sortBy(x=>x)
//
//    val groupRDD= word.groupBy(word=>word.endsWith("A"))
////    println(sortRDD., groupRDD.collect())
//
//    sortRDD.collect().foreach(println)
//    groupRDD.collect().foreach(println);
////    val noheader= df.filter(contains(header))
//    df.show()
//    df.printSchema()
//   //referencing the specific columns
//    df.select("Mail").show();
//    import spark.implicits._
//    $"Mail"
//    df.select(df("Status"),$"Mail").show();
//    val newcolumn=concat(df("Status"),lit("hello world"))
//    val column= df("Status")
////    val columnString=column.cast(StringType).as("OpenAsString")
//    expr("cast(current_timestamp() as string)")
////    val timestamp=current_timestamp().cast(StringType)
//
//
////    df.select("Status",newcolumn,"Mail")
////    newcolumn.show()
////    import spark.implicits._
//
////    df.first()



    spark.sparkContext.setLogLevel("ERROR")

  }
}



