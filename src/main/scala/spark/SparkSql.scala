package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_replace, max}

object SparkSql extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("Spark SQL")
    .getOrCreate()

  import spark.implicits._

  case class emp(
                EmpId: Int,
                EmpName: String
                )

  val empDF = Seq(emp(1, "Tom"),
    emp(2, "Harry"),
    emp(2, "Harry Lewis"),
    emp(3, "Hermoine")
  ).toDF()

  empDF.show(false)

//  +-----+-----------+
//  |EmpId|EmpName    |
//  +-----+-----------+
//  |1    |Tom        |
//  |2    |Harry      |
//  |2    |Harry Lewis|
//  |3    |Hermoine   |
//  +-----+-----------+


  val empDF1 = empDF.groupBy("EmpId").agg(max("EmpName").alias("EmpName"))

  val resultDF = empDF1
    .withColumn("EmpNameNew", regexp_replace(col("EmpName"), " ", ""))
    .drop("EmpName")
    .withColumnRenamed("EmpNameNew", "EmpName")
    .orderBy(col("EmpId").asc)


  resultDF.show(false)

//  +-----+----------+
//  |EmpId|EmpName   |
//  +-----+----------+
//  |1    |Tom       |
//  |2    |HarryLewis|
//  |3    |Hermoine  |
//  +-----+----------+

}
