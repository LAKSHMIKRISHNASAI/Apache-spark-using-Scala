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
   val simpleData = Seq(("James", "Sales", 3000),
     ("Michael", "Sales", 4600),
     ("Robert", "Sales", 4100),
     ("Maria", "Finance", 3000),
     ("James", "Sales", 3000),
     ("Scott", "Finance", 3300),
     ("Jen", "Finance", 3900),
     ("Jeff", "Marketing", 3000),
     ("Kumar", "Marketing", 2000),
     ("Saif", "Sales", 4100)
   )
   import spark.implicits._
    // window functions
   val df = simpleData.toDF("employee_name", "department", "salary")
   df.show()
   val windowspec= Window.partitionBy("department").orderBy("salary")
   window functions-rank,row_number,dense_rank,percent_rank,ntile,cume_dist
   val windowspec=Window.partitionBy("department").orderBy("salary")
   df.withColumn("row number:", row_number.over(windowspec)).show()
   df.withColumn("rank:" , rank.over(windowspec)).show()
   df.withColumn("dense rank:", dense_rank.over(windowspec)).show()
   df.withColumn("ntile:", ntile(3).over(windowspec)).show()
   df.withColumn("cummulative distribution", cume_dist.over(windowspec)).show()
      df.withColumn("row number:", row_number.over())

   val data= Seq(
     Row("james","cameron",List(Row("Newyork","NY"),Row("brooklyn","NY")),Map("hair"->"black","eye"->"brown"),Map("height"->"5.9")))
     Row("johny","depp",List(Row("Sanjose","CA"),Row("Sandigo","CA")),Map("hair"->"brown","eye"->"black"),Map("height"->"5.8")),
     Row("andrew","sanjose",List(Row("orange","CA"),Row("lasvegas","NV")),Map("hair"->"red","eye"->"black"),Map("height"->"5.10")),
     Row("Maria","andrew",List(Row("Texas","TX"),Row("Lasvegas","NV")),Map("hair"->"blond","eye"->"red"),Map("height"->"5.6")),
     Row("Jen","mathew",List(Row("LAX","CA"),Row("Orange","CA")),Map("hair"->"black","eye"->"black"),Map("height"->"5.2")))


   data.foreach(println)

    
       val schema= new StructType()
     .add("name", new StructType())
//      .add("fname",StringType)
     .add("lname",StringType))
   val arrschema= StructType(
     Seq(StructField("fname",StringType:StringType,nullable=true),
//        StructField("lname",StringType:StringType,nullable=true),
       StructField("address",ArrayType:(StringType),true),
       StructField("city",StringType:StringType,nullable=true),
       StructField("state",StringType:StringType,nullable=true),
       StructField("properties",MapType:StringType,nullable = true),
         StructField("second properties",MapType:StringType,nullable = true)
//    ))

  }


}
