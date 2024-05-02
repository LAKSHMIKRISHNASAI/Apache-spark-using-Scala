import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object json {
  def main(args:Array[String]):Unit={
    val spark= SparkSession.builder().appName("read json files").master("local[2]").getOrCreate()
    val data=spark.read.option("multiline","true").json("D:/new_downloads/scala/spark_demo_app/src/main/scala/data.json")
    data.show(false)
    val schema= new StructType()
      .add("current city",StringType,nullable=true)
      .add("Country",StringType,nullable=true)
      .add("key",IntegerType,nullable=true)
      .add("Name",StringType,nullable=true)
      .add("State",StringType,nullable=true)
    val json_data= spark.read.schema(schema).json("D:/new_downloads/scala/spark_demo_app/src/main/scala/data.json")
    json_data.show()


    import org.apache.spark.sql.SaveMode
   val df=data.write.mode(SaveMode.Overwrite).json("D:/new_downloads/scala/spark_demo_app/src/main/scala/data.json")
//    df.ensuring(StringType)

  }
}
