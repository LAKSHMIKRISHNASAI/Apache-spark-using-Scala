//package src.main.scala
//package myapp
import org.apache.spark.sql.SparkSession
object app3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("app3")
      .getOrCreate()

    val countries= Map(("USA","UNITED STATES OF AMERICA"),("FL","FLORIDA"),("IND","INDIA"));
    val states=Map(("CA","CALIFORNIA"),("LA","LOSS ANGELS"),("NY","NEW YORK"),("DL","DELHI"));

    val broadcastStates= spark.sparkContext.broadcast(states);
    val broadcastCountries= spark.sparkContext.broadcast(countries);

    val data= Seq(("john","smith","USA","CA"),
      ("Micheal","jackson","USA","NY"),("Robert","Williams","USA","CA"),
    ("Maria","Jones","IND","DL")
    )
    val rdd= spark.sparkContext.parallelize(data)
    rdd.collect()
    val rdd2= rdd.map(col=>{
      val country= col._3;
      val state=col._4

      val fullcountry=broadcastCountries.value.get(country);
      val fullstate= broadcastStates.value.get(state)
      (col._1,col._2,fullcountry,fullstate)
    })
    println(rdd2.collect().mkString("\n"));

    //broadcast variables are read-only variables which are cached within all the nodes inorder to need of tasks. Instead of sending all the data to the tasks, the spark broadcast the variables to the tasks executed within the efficient
    // broadcast algorithms. The running of SparkRDD and dataframe jobs defined by using Spark broadcast means, spark makes the following tasks:
    //1) spark breaks the jobs into stages by the distributed partitions and actions are executed within the stages
    // 2) spark then breaks the stages into tasks
    //3) spark broadcast the common data to the needed tasks within the stage.
    // 4) The broadcasted data is cached in the serialized format and deserialized after executing the task.



    val broadcaststates2= spark.sparkContext.broadcast(states)
    val broadcastcountires2= spark.sparkContext.broadcast(countries)


    val columns= Seq("firstname","lastname","country","state")
    import spark.sqlContext.implicits._
    val df= data.toDF(columns:_*)
    df.show()
    val df2= df.map(row=>{
      val state= row.getString(3);
      val country= row.getString(2);
      val fullcountry= broadcastcountires2.value.get(country)
      val fullstate= broadcaststates2.value.get(state)
      (row.getString(0),row.getString(1),fullcountry,fullstate)
    })
    df2.show()
    val df3= df2.toDF(columns:_*)
    df3.show()

//    val inputRDD= spark.sparkContext.parallelize(List(("Z",1),("A",20),("D",12),("S",13),("G",16)));
//    val listRDD= spark.sparkContext.parallelize(List(1,2,4,5,6));
//    //    aggregate function
//    def param1= (accu:Int,v:Int)=>accu+v;
//    def param2=(acc1:Int,acc2:Int)=>acc1+acc2;
//    println(listRDD.aggregate(0)(param1,param2))
//    val data= List("INDIA","USA","INDIA","GERMANY","AUSTRALIA","USA","FRANCE","GERMANY","INDIA","NEW ZEALAND")
//    val separate= data.flatMap(_.split(" "))
//    val wordrdd= spark.sparkContext.parallelize(List("INDIA","USA","INDIA","GERMANY","AUSTRALIA","USA","FRANCE","GERMANY","INDIA","NEW ZEALAND"))
//    val separate= wordrdd.flatMap(word=>word.split(" "))
//
//    val word=separate.map(word=>(word,1));
//    val count= word.reduceByKey(_+_);
//    count.collect().foreach(println)
//
//    //repartition() method used to increase or decrease the partitions.
//    //coalesce() used to decrease the number of partitions.
//    val data2= spark.sparkContext.parallelize(Range(0,20),6)
//    //to know the size and length of the partitions
//    println("number of partitions are:"+data2.partitions.size);
////    data2.saveAsTextFile("D://new_downloads//scala//spark_demo_app//src//main//scala//");
//    val data3= data2.repartition(4);
//    data3.collect()
//    println(data3.partitions.size);
//    data2.coalesce(3).collect();
//
//    println("number of partitions reduced are:" ,rdd2.partitions.size)
//
////    data2.saveAsTextFile("C://Users")



  }
}



