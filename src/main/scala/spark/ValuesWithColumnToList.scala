package spark

import org.apache.spark.sql.SparkSession

object ValuesWithColumnToList extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("DataFrame-example")
    .getOrCreate()

  import spark.implicits._

  case class M1(c1: String, c2: String)

  // Create DataFrame
  val df = Seq(
    M1("a1", "b1"),
    M1("a2", "b2"),
    M1("a3", "b3"),
    M1("a4", "b4"),
    M1("a5", "b5")
  ).toDF("col1", "col2")

 df.show(false)
//  Result show DataFrame df
//  +----+----+
//  |col1|col2|
//  +----+----+
//  |a1  |b1  |
//  |a2  |b2  |
//  |a3  |b3  |
//  |a4  |b4  |
//  |a5  |b5  |
//  +----+----+
//
  // Values with column to list
  val l = df.select('col1).as[String].collect.toList
  println(
    s"""~~~~~>
       |List values with column:  $l
       |~~~~~<""".stripMargin)
// result
//  List values with column:  List(a1, a2, a3, a4, a5)
//
}
