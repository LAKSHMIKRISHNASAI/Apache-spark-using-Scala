import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._
import scala.math.Ordered.orderingToOrdered
object hello {
  def main(args:Array[String]): Unit = {

//    println("finally running scala")
//    val spark= SparkSession.builder().appName("myfirstapp").master("local[2]").getOrCreate();
//    val conf= new SparkConf().setAppName("myfirstapp").setMaster("local[2]");
//    val sc= new SparkContext(conf);
//    val file= sc.textFile("D://new_downloads//scala//spark_demo_app//src//main//scala//flight_price_prediction.csv").cache();
////    val word_count=file.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
//////      reduceByKey((x,y)=>(x+y));
////    word_count.collect().foreach(println)
//////    word_count.saveAsTextFile("D://new_downloads//scala//spark_demo_app"+java.util.UUID.randomUUID.toString);
////    sc.stop()
//   val rdd= file.map(line=>line.split(","))
//    val rdd2=rdd.map(a=>a(1),a.mkString(","))


    val spark= SparkSession.builder().appName("partitions").master("local[3]").getOrCreate();
//    val data=spark.sparkContext.textFile("D://new_downloads//scala//spark_demo_app//src//main//scala//flight_price_prediction.csv")
//    val rdd=data.map(line=>line.split(","))
//    val rdd2=rdd.map(a=>(a(1),a.mkString(",")));
//    val rdd3=rdd2.partitionBy(new HashPartitioner(3))
//    rdd3.take(5).foreach(println)

    import org.apache.spark.util.LongAccumulator
    import org.apache.spark.util.{CollectionAccumulator,DoubleAccumulator}
//    val accum=spark.sparkContext.longAccumulator("sumAccumulator")
//    val rdd= spark.sparkContext.parallelize(Array(12,14,16,18,20,22,24,26,28,30 ))
//    def longaccum:(Int,Int)=>Int= (acc1:Int,acc2:Int)=>acc1+acc2;
//    def  longaccum2:(Int,Int)=>Int=(acc:Int,v:Int)=>acc+v;
//    println(rdd.aggregate(0)(longaccum,longaccum2))
//    def longacc(x:Int,y:Int, f:(Int,Int)=>Int):Int=f(x,y):Int;
//    println(longacc(50,30,(x,y)=>(x+y)));
//    rdd.foreach(x=>accum.add(x));
//    println(accum.value)
//    //accumulator has various methods which are
//    // copy(), merge(), count(), value(), add(),sum(),avg(), isZero(), reset().
////    rdd.foreach(x=>accum.count(x:Any))
//    val data= spark.sparkContext.parallelize(List((10,12),(14,16),(18,20),(24,26),(28,30)))
//    data.sortBy(f=>f).foreach(println)
//    import spark.implicits._
//    import org.apache.spark.sql.functions._
//    import scala.math.Ordered.orderingToOrdered
//    data.groupBy(key=>key)

    val  arrayStructure=Seq(
      Row(Row("leon", "","james"),List("java","python","scala","c++"),"OH","M"),
      Row(Row("Annie","","Rose"),List("spark","java","c++"),"NY","F"),
      Row(Row("Julia","","williams"),List("C#","VB"),"OH","F"),
      Row(Row("Julia", "", "Williams"), List("CSharp", "VB"), "OH", "F"),
      Row(Row("Maria", "Anne", "Jones"), List("CSharp", "VB"), "NY", "M"),
      Row(Row("Jen", "Mary", "Brown"), List("CSharp", "VB"), "NY", "M"),
      Row(Row("Mike", "Mary", "Williams"), List("Python", "VB"), "OH", "M"));
    arrayStructure.foreach(println)
    import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType}
    import org.apache.spark.sql.types.ArrayType
    val arrschema= new StructType().
      add("name",new StructType()
       .add("Firstname", StringType)
       .add("Middlename",StringType)
       .add("Lastname",StringType))
      .add("languages",ArrayType(StringType))
      .add("state",StringType)
      .add("gender",StringType)
//
    val df2= spark.createDataFrame(spark.sparkContext.parallelize(arrayStructure),arrschema)
    df2.show()
    df2.printSchema()
//    df.collect().foreach(println)
    df2.where(df2("state")==="OH" && df2("gender")==="M").show(false)
//    df.where(df("Firstname")==="john").show()
////    df.show()
val data = Seq(Row(Row("James;","","Smith"),"36636","M","3000"),
  Row(Row("Michael","Rose",""),"40288","M","4000"),
  Row(Row("Robert","","Williams"),"42114","M","4000"),
  Row(Row("Maria","Anne","Jones"),"39192","F","4000"),
  Row(Row("Jen","Mary","Brown"),"","F","-1")
)

    val schema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("dob",StringType)
      .add("gender",StringType)
      .add("salary",StringType)

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
    df.show()
    import org.apache.spark.sql.functions.col

    df.withColumn("firstname",col("firstname")).show()













  }

}
