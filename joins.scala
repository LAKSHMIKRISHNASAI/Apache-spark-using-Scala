import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.{FullOuter, LeftAnti, LeftOuter, RightOuter}
import org.apache.spark.sql.functions.substring
object joins {
  def main(args: Array[String]):Unit={
    val spark= SparkSession.builder().appName("joins").master("local[2]").getOrCreate()
    val emp = Seq((1,"Smith",-1,"2018","10","M",3000),
      (2,"Rose",1,"2010","20","M",4000),
      (3,"Williams",1,"2010","10","M",1000),
      (4,"Jones",2,"2005","10","F",2000),
      (5,"Brown",2,"2010","40","",-1),
      (6,"Brown",2,"2010","50","",-1)
    )
    val empColumns = Seq("emp_id","name","superior_emp_id","year_joined",
      "emp_dept_id","gender","salary")
    import spark.implicits._
    val empdf=emp.toDF(empColumns:_*)
    empdf.show()

    val dept = Seq(("Finance",10),
      ("Marketing",20),
      ("Sales",30),
      ("IT",40)
    )

    val deptColumns = Seq("dept_name","dept_id")
    val deptdf=dept.toDF(deptColumns:_*)
    deptdf.show()
    val left_join=empdf.join(deptdf,empdf("emp_dept_id")===deptdf("dept_id"),"left")
    left_join.show()
    val right_join= empdf.join(deptdf,empdf("emp_dept_id")===deptdf("dept_id"),"right")
    val right_outer_join= empdf.join(deptdf,empdf("emp_dept_id")===deptdf("dept_id"),"rightOuter")
    empdf.join(deptdf,empdf("emp_dept_id")===deptdf("dept_id"),"outer").show()
    empdf.join(deptdf,empdf("emp_dept_id")===deptdf("dept_id"),"fullouter").show()
    import org.apache.spark.sql.functions.col
    empdf.as("emp1").join(deptdf.as("emp2"),col("emp1.emp_dept_id")===col("emp2.dept_id"),"inner").select(col("emp1.emp_id"),
      col("emp1.name"), col("emp2.dept_id").as("department_id"),col("emp2.dept_name").as("departement_name")).show()

    empdf.as("emp1").join(deptdf.as("emp2"),col("emp1.emp_dept_id")===col("emp2.dept_id"),"leftsemi").show()

    //user defined functions
    val columns = Seq("Seqno","Quote")
    val data = Seq(("1", "Be the change that you wish to see in the world"),
      ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
      ("3", "The purpose of our lives is to be happy.")

    )
    import spark.implicits._

    val df = data.toDF(columns:_*)
    val convertCase=(str:String)=>{
      val arr= str.split(",")
      arr.map(word=>word.substring(0,1).toUpperCase + word.substring(1).mkString(""))
    }
//    convertCase(data:Seq[(String,String)])
    import org.apache.spark.sql.functions.udf
    val convertcase= udf(convertCase)
    df.select(col("seqno"),convertcase(col("Quote"))).collect().foreach(println)







  }

}
