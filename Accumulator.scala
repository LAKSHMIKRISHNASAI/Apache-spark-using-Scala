import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
  import org.apache.spark.util.{CollectionAccumulator,DoubleAccumulator}
object accumulator{
  def main(args:Array[String]):Unit={
  val spark= SparkSession.builder().appName("accumulator").master("local[2]").getOrCreate()
   val accum=spark.sparkContext.longAccumulator("sumAccumulator")
   val rdd= spark.sparkContext.parallelize(Array(12,14,16,18,20,22,24,26,28,30 ))
   def longaccum:(Int,Int)=>Int= (acc1:Int,acc2:Int)=>acc1+acc2;
   def  longaccum2:(Int,Int)=>Int=(acc:Int,v:Int)=>acc+v;
   println(rdd.aggregate(0)(longaccum,longaccum2))
   def longacc(x:Int,y:Int, f:(Int,Int)=>Int):Int=f(x,y):Int;
   println(longacc(50,30,(x,y)=>(x+y)));
   rdd.foreach(x=>accum.add(x));
   println(accum.value)
   //accumulator has various methods which are
   // copy(), merge(), count(), value(), add(),sum(),avg(), isZero(), reset().
//    rdd.foreach(x=>accum.count(x:Any))
   val data= spark.sparkContext.parallelize(List((10,12),(14,16),(18,20),(24,26),(28,30)))
   data.sortBy(f=>f).foreach(println)
   import spark.implicits._
   import org.apache.spark.sql.functions._
   import scala.math.Ordered.orderingToOrdered
   data.groupBy(key=>key)
  }
}
