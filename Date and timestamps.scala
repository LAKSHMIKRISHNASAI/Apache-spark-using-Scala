    import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
object timestamp{
  def main(args:Array[String]):Unit={
     // import org.apache.spark.sql.expressions.Window
    val spark= SparkSession.builder().appName("window functions").master("local[2]").getOrCreate()
   val arraystructure= new StructType()
     .add("firstname",StringType)
     .add("lastname",StringType)
     .add("place",ArrayType(new StructType().add("city",StringType).add("state",StringType))
     .add("properties",MapType(StringType,StringType))
     .add("second_property",MapType(StringType,StringType)))
    
    import spark.implicits._
val data= Seq(("2012-01-14"))
   data.toDF("input").select(col("input"),current_date() as ("current_date"),
     date_format(col("input"),"MM-dd-yyyy").as("format")).show()
   data.toDF("inputs").select(col("inputs"),current_date(),date_format(col("inputs"), "mm-dd-yyyy").as("format")).show()

   val data2= Seq(("2012-02-21"),("2019-06-24"),("2019-09-20"))
   data2.toDF("input2").select(col("input2"),datediff(current_date(),col("input2")).as("diff")).show()
//    months between
   data2.toDF("input3").select(col("input3"),datediff(current_date(),col("input3")).as("datediff"),
   months_between(current_date(),col("input3")).as("months between")).show()
   data2.toDF("date").select(col("date"),
     trunc(col("date"),"Month").as("month"),
     trunc(col("date"),"Year").as("year"),
     add_months(col("date"),4).as("months added"),
     date_add(col("date"),4).as("date added"),
     next_day(col("date"),"sunday").as("next day"),
     dayofyear(col("date").as("day of year")),
     dayofmonth(col("date")).as("month of the day"),
     dayofweek(col("date")).as("day of week")).show()

   val date= Seq(1).toDF("seq")
   date.withColumn("current timestamp:", current_timestamp().as("timestamp"))
     .withColumn("current date:",current_date().as("current date")).show()

   val curr_date=Seq(1).toDF("curr_date")
//    val df= spark.read.format("xml").option("rowTag","person").load("")
//    val df3= spark.sparkContext.parallelize(List("Germany India USA","USA India Russia","India Brazil Canada China"))
//    val rdd=df3.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
//    rdd.collect().foreach(println)
//    rdd.sortByKey().foreach(println)
// //    rdd.countByvalue()
//    def param1(acc1:Int,acc2:Int)=acc1+acc2
//    def param2(accu1:Int,accu2:Int):Int=accu1+accu2

//    rdd.aggregateByKey(0)(param1,param2).foreach(println)
//    rdd.keys.foreach(println)
//    rdd.values.foreachPartition(println)
  }
}
