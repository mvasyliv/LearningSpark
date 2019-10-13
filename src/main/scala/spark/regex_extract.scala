package spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.matching.Regex

object regex_extract extends  App{

  val spark = SparkSession.builder()
    .master("local")
    .appName("DataFrame-example")
    .getOrCreate()

  import spark.implicits._
  val df = List("vmv_gaz@yahoo.com,vasyliv@gmail.com ad@gmail.com-rrrrrrr user-name@domain.co.in adr@gmail.comrrrrrrr ttttttttt", "test", "test@yahoo.com")
    .toDF("col1")
  val reg3 = """[a-zA-Z0-9._-][a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,3}""".r

  val emails = f(spark, df,  reg3)
  println("~~~~~> regex extract after function")
  emails.show(false)


  println("~~~~~> regex extract after ds")
  val eDS = List("vmv_gaz@yahoo.com,vasyliv@gmail.com ad@gmail.com-rrrrrrr user-name@domain.co.in adr@gmail.comrrrrrrr ttttttttt", "test", "test@yahoo.com")
    .toDS.as[String]
  val res = eDS.flatMap{x1 => reg3.findAllIn(x1)}.toDF("col2")
  res.show(false)
  def f(spark: SparkSession, data: DataFrame, reg: Regex): DataFrame = {
    import spark.implicits._
    val ds = data.as[String]
    ds.flatMap{x1 => reg.findAllIn(x1)}.toDF("col2")
  }
}
