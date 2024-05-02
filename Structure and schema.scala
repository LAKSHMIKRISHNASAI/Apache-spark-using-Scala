import org.apache.spark.sql.types._
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
   df2.where(df("Firstname")==="john").show()
   df2.show()
