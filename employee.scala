import org.apache.spark.sql.SparkSession.setActiveSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructType}
object employee {
  case class student(name:String,college:String)
  case class employee(key:String, fname:String, lname: String, dept:String,address:String,city:String,state:String)
  def main(args:Array[String]):Unit={
    // defining employee data
   val data = Seq(employee("1","andrew","thomas","R&D","ameerpet","hyderabad","telangana"),
     employee("2","john","schwez","CS","begampet","hyderabad","telangana"),
     employee("3","annie","rose","Product development","nizampet","hyderabad","telangana"),
     employee("4","tom","crusie","R&D","banjara hills","secundrabad","telangana"),
     employee("5","ammie","jackson","customer support","nizampet","hyderabad","telangana"))
    val spark= SparkSession.builder().appName("prediction").master("local[2]").getOrCreate()
    setActiveSession(spark)
   val df=spark.createDataFrame(spark.sparkContext.parallelize(data))
   import org.apache.spark.sql.functions.col
   df.select(col("fname"),col("lname"),col("dept"),col("address"),col("city")).filter(col("address")==="nizampet" && df("city")==="hyderabad").as("condition").show()
   df.createOrReplaceTempView("emp")
   df.select(col("fname"),col("lname"),col("dept"),col("city")).filter(col("dept")==="R&D").show()
   df.sqlContext.sql("""select city,count(dept) as dept_count from emp group by city""").show()
   df.show()


    
   import org.apache.spark.rdd.RDD
   def stockparse(s1:String):student={
     val line= s1.split(" ")
      student(line(0),line(1))
   }

   def parserdd(rdd:RDD[String]):RDD[student]={
     val header= rdd.first()
     rdd.filter(_(0)!=header(0)).map(stockparse)
   }
   val stream=spark.readStream.format("console").option("host","localhost").option("port",4040).load()
   stream.show()

    
   val rdd= spark.read.option("header",true).option("inferSchema",true).csv("D://new_downloads//Fake.csv//Fake.csv");

   case class stock(id:Integer,review:String,liked:Integer,overview:String)
   rdd.show();
   def parseStock(str:String):stock={
     val line= str.split(" ")
     stock(line(0).toInt,line(1),line(2).toInt,line(3))
   }
//    parseStock()
   import org.apache.spark.rdd.RDD
   def parseRdd(rdd:RDD[String]):RDD[stock]={
     val header= rdd.first
     rdd.filter(_(0)!=header(0)).map(parseStock).cache()
   }
    import org.apache.spark.rdd.RDD
   val splitting=(str:RDD[String])=>{
     val line= str.first()
     (line(1)).toUpper
   }
    import spark.implicits._
   val rdd3=parseRdd(spark.sparkContext.textFile("D:/new_downloads/restaurant_analysis.csv"))
   rdd3.collect().foreach(println)
   val rdd2=spark.read.option("header",true).csv("D:/new_downloads/restaurant_analysis.csv")
   rdd2.show()
   splitting(rdd2).
   rdd2.collect().foreach(println)
       // case class employees()

//
//    val structureData = Seq(
//      Row("36636","Finance",Row(3000,"USA")),
//      Row("40288","Finance",Row(5000,"IND")),
//      Row("42114","Sales",Row(3900,"USA")),
//      Row("39192","Marketing",Row(2500,"CAN")),
//      Row("34534","Sales",Row(6500,"USA"))
//    )
//    import scala.collection.mutable
//    import org.apache.spark.sql.functions.{map,map_keys,map_values,map_entries,map_concat,map_from_entries,map_from_arrays}
//    val schema= new StructType()
//      .add("name",StringType)
//      .add("department",StringType)
//      .add("address",new StructType()
//        .add("amount",IntegerType)
//        .add("country",StringType)
//      )
//
//    val df= spark.createDataFrame(spark.sparkContext.parallelize(structureData),schema)



    // MAP FUNCTIONS NEW EXAMPLE
    val data= Seq(
      Row("james","cameron",List(Row("Newyork","NY"),Row("brooklyn","NY")),Map("hair"->"black","eye"->"brown"),Map("height"->"5.9")),
    Row("johny","depp",List(Row("Sanjose","CA"),Row("Sandigo","CA")),Map("hair"->"brown","eye"->"black"),Map("height"->"5.8")),
    Row("andrew","sanjose",List(Row("orange","CA"),Row("lasvegas","NV")),Map("hair"->"red","eye"->"black"),Map("height"->"5.10")),
    Row("Maria","andrew",List(Row("Texas","TX"),Row("Lasvegas","NV")),Map("hair"->"blond","eye"->"red"),Map("height"->"5.6")),
    Row("Jen","mathew",List(Row("LAX","CA"),Row("Orange","CA")),Map("hair"->"black","eye"->"black"),Map("height"->"5.2")))

    val arraystructure= new StructType()
          .add("firstname",StringType)
          .add("lastname",StringType)
          .add("place",ArrayType(new StructType().add("city",StringType).add("state",StringType)))
          .add("properties",MapType(StringType,StringType))
         .add("second_property",MapType(StringType,StringType))
    val df=spark.createDataFrame(spark.sparkContext.parallelize(data),arraystructure)
    df.show()
//    println("map keys"+ df.select(col("properties")))
    import org.apache.spark.sql.functions._
    df.withColumn("map_keys", map_keys(df("properties"))).show()
    df.withColumn("map_values",map_values(df("properties"))).show()
    df.withColumn("map_concat",map_concat(df("properties"),col("second_property"))).show()
    df.collect().foreach(println)
    df.select(("place"))
    import org.apache.spark.sql.functions.split
    df.withColumn("new place", split(df("place")," ")(0))

////    df("place").apply(line=>line.split(" "))
//    def splitting(str:String)={
//      val split= str.split(" ")
//      split(2)
//    }

//    df.withColumn("",df("place"))




  }

}
