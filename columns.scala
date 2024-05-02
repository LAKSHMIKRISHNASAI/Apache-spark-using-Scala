
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{approx_count_distinct, col, concat, lit}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
object columns {
  def main(args:Array[String]):Unit={
    val spark= SparkSession.builder().appName("columns").master("local[*]").getOrCreate()

//    val data= Seq(("olivia","morris","1995,28,F",1200000),
//      ("john","abraham","1982,44,M",25000000),
//      ("jr","ntr","1985,38,M",40000000),
//      ("ram","charan","1986,37,M",60000000),
//      ("alia","bhat","1992,32,F",20000000))
//    data.collect().foreach(println)
//    val schema= new StructType().add("name", new StructType().add("firstname",StringType).add("lastname",StringType))
//      .add("year",StringType)
//      .add("age",IntegerType)
//      .add("gender",StringType)
//      .add("salary",IntegerType)
//    val col= Seq("fname","lname","bio","salary")
//    import spark.implicits._
//    val df=spark.createDataFrame(data).toDF(col:_*)
//
//    val newdf=df.map(f=>{
//      val biosplit=f.getAs[String](2).split(",")
//      val fname=f.getAs[String](0)
//      val lname=f.getAs[String](1)
//      val salary=f.getAs[Integer](3)
//      (fname,lname,biosplit(0),biosplit(1),biosplit(2),salary)
//    })
//
//   val df3= newdf.toDF("First name","surname","year","Age","Gender","salary")
//    df3.show()
//    df3.withColumn("country",lit("Australia")).show()
//    df3.withColumn("Fullname",concat(df3("First name"),lit(" "),df3("surname"))).show()
//    newdf.toDF("First name","surname","year","Age","Gender","salary").withColumn("Fullname",concat(col("First name
//    )))
//    newdf.show()
//    df.collect().foreach(println)
//    df.show()
//
//    df.withColumn("salary",col("salary")*10).show()
//    df.withColumnRenamed("name","Actor Name")
//    df.withColumn("year",df("year").cast(IntegerType))
//    df.withColumn("country",lit("USA"))
//    df.withColumnsRenamed(("year","DOB"),("gender","Gender"))

//    val columns= Seq("name","address")
//    val data2= Seq(("robert,smith","1 main road, newark, NJ,923347"),("john, andrews","3456 walnut road, newark,NJ, 94732"))

//    val df2= data2.map(word=>word.split(","))
//    datashow()


//
//    import spark.implicits._
//    val df2=spark.createDataFrame(data2).toDF(columns:_*)
//    df2.show()
//    val df3= df2.map(f=>{
//      val namessplit= f.getAs[String](0).split(",")
//      val addsplit=f.getAs[String](1).split(",")
//      (namessplit(0),namessplit(1),addsplit(0),addsplit(1),addsplit(2),addsplit(3))
////      (f.getString(0),f.getString(1),)
//    })
//    val newdf= df3.toDF("fname","lname","Address lane1","city","state","zipcode")
//    newdf.show()
//    df2.show()
//    var d=spark.createDataFrame(data2).toDF(columns:_*)
//
//    df3.map()
    val map1:Map[Int,Int]=Map(1->10,2->20,3->30,4->40)
    println(map1)
    val data3= Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))
    import spark.implicits._
    val column=Seq("Product","amount","country")
    val df=data3.toDF(column:_*)
    df.show()
    df.groupBy(df("country")).count().show()
//    df.groupBy("Product").pivot("country").sum("amount").show()
    import org.apache.spark.sql.functions.expr
    df.groupBy("Product","country").sum("amount").groupBy("Product").pivot("country").sum("sum(amount)").show()
   df.na.drop().show(false)

    df.select(approx_count_distinct("amount")).collect()(0)(0)
    import org.apache.spark.sql.functions._
    df.select(skewness("amount")).show()
//    aggregate functions
//    min,max,mean,std,approx_count_distinct,collect_list,collect_set,
//    skewness, first_value,first,last,count,distinct,avg, variance, var_samp(),corr(),



  }


}
