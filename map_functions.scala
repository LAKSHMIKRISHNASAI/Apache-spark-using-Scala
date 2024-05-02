import org.apache.hadoop.shaded.org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.months_between
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructField, StructType}
object map_functions {
  def main(args:Array[String]):Unit={
//    println(current_timestamp())
    import org.apache.spark.sql.expressions.Window
    val spark= SparkSession.builder().appName("window functions").master("local[2]").getOrCreate()
//    val simpleData = Seq(("James", "Sales", 3000),
//      ("Michael", "Sales", 4600),
//      ("Robert", "Sales", 4100),
//      ("Maria", "Finance", 3000),
//      ("James", "Sales", 3000),
//      ("Scott", "Finance", 3300),
//      ("Jen", "Finance", 3900),
//      ("Jeff", "Marketing", 3000),
//      ("Kumar", "Marketing", 2000),
//      ("Saif", "Sales", 4100)
//    )
//    import spark.implicits._
//    val df = simpleData.toDF("employee_name", "department", "salary")
//    df.show()
//    val windowspec= Window.partitionBy("department").orderBy("salary")
//    window functions-rank,row_number,dense_rank,percent_rank,ntile,cume_dist
//    val windowspec=Window.partitionBy("department").orderBy("salary")
//    df.withColumn("row number:", row_number.over(windowspec)).show()
//    df.withColumn("rank:" , rank.over(windowspec)).show()
//    df.withColumn("dense rank:", dense_rank.over(windowspec)).show()
//    df.withColumn("ntile:", ntile(3).over(windowspec)).show()
//    df.withColumn("cummulative distribution", cume_dist.over(windowspec)).show()
//    //    df.withColumn("row number:", row_number.over())
//    val data= Seq(
//      Row("james","cameron",List(Row("Newyork","NY"),Row("brooklyn","NY")),Map("hair"->"black","eye"->"brown"),Map("height"->"5.9")))
//      Row("johny","depp",List(Row("Sanjose","CA"),Row("Sandigo","CA")),Map("hair"->"brown","eye"->"black"),Map("height"->"5.8")),
//      Row("andrew","sanjose",List(Row("orange","CA"),Row("lasvegas","NV")),Map("hair"->"red","eye"->"black"),Map("height"->"5.10")),
//      Row("Maria","andrew",List(Row("Texas","TX"),Row("Lasvegas","NV")),Map("hair"->"blond","eye"->"red"),Map("height"->"5.6")),
//      Row("Jen","mathew",List(Row("LAX","CA"),Row("Orange","CA")),Map("hair"->"black","eye"->"black"),Map("height"->"5.2")))


//    data.foreach(println)


    //    val schema= new StructType()
//      .add("name", new StructType())
////      .add("fname",StringType)
//      .add("lname",StringType))
//    val arrschema= StructType(
//      Seq(StructField("fname",StringType:StringType,nullable=true),
////        StructField("lname",StringType:StringType,nullable=true),
//        StructField("address",ArrayType:(StringType),true),
//        StructField("city",StringType:StringType,nullable=true),
//        StructField("state",StringType:StringType,nullable=true),
//        StructField("properties",MapType:StringType,nullable = true),
//          StructField("second properties",MapType:StringType,nullable = true)
//    ))
    import spark.implicits._
//    spark.createDataFrame(spark.sparkContext.parallelize(data),arrschema).show()





//
//
    import org.apache.spark.sql.types.MapType
//    val arraystructure= new StructType()
//      .add("firstname",StringType)
//      .add("lastname",StringType)
//      .add("place",ArrayType(new StructType().add("city",StringType).add("state",StringType))
//      .add("properties",MapType(StringType,StringType))
//      .add("second_property",MapType(StringType,StringType)))
//val data= Seq(("2012-01-14"))
//    data.toDF("input").select(col("input"),current_date() as ("current_date"),
//      date_format(col("input"),"MM-dd-yyyy").as("format")).show()
//    data.toDF("inputs").select(col("inputs"),current_date(),date_format(col("inputs"), "mm-dd-yyyy").as("format")).show()
//
//    val data2= Seq(("2012-02-21"),("2019-06-24"),("2019-09-20"))
//    data2.toDF("input2").select(col("input2"),datediff(current_date(),col("input2")).as("diff")).show()
////    months between
//    data2.toDF("input3").select(col("input3"),datediff(current_date(),col("input3")).as("datediff"),
//    months_between(current_date(),col("input3")).as("months between")).show()
//    data2.toDF("date").select(col("date"),
//      trunc(col("date"),"Month").as("month"),
//      trunc(col("date"),"Year").as("year"),
//      add_months(col("date"),4).as("months added"),
//      date_add(col("date"),4).as("date added"),
//      next_day(col("date"),"sunday").as("next day"),
//      dayofyear(col("date").as("day of year")),
//      dayofmonth(col("date")).as("month of the day"),
//      dayofweek(col("date")).as("day of week")).show()
//
//    val date= Seq(1).toDF("seq")
//    date.withColumn("current timestamp:", current_timestamp().as("timestamp"))
//      .withColumn("current date:",current_date().as("current date")).show()
//
//    val curr_date=Seq(1).toDF("curr_date")
////    val df= spark.read.format("xml").option("rowTag","person").load("")
//    val df3= spark.sparkContext.parallelize(List("Germany India USA","USA India Russia","India Brazil Canada China"))
//    val rdd=df3.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
//    rdd.collect().foreach(println)
//    rdd.sortByKey().foreach(println)
////    rdd.countByvalue()
//    def param1(acc1:Int,acc2:Int)=acc1+acc2
//    def param2(accu1:Int,accu2:Int):Int=accu1+accu2
//
//    rdd.aggregateByKey(0)(param1,param2).foreach(println)
//    rdd.keys.foreach(println)
//    rdd.values.foreachPartition(println)





//      trunc(col("date"),"Day").as("day")).show()








  }


}
