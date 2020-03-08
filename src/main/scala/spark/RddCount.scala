package spark

import org.apache.spark.sql.SparkSession

object RddCount extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("RDD count")
    .getOrCreate()

  val r = spark.range(1)

  println(s"time stamp 0 -> ${System.currentTimeMillis().toString}")
  val cnt = r.rdd.count() //have to use count to trigger the get rdd lazy val
  println(s"cnt = $cnt")
  println(s"time stamp 1 -> ${System.currentTimeMillis().toString}")


  println(s"time stamp 0 -> ${System.currentTimeMillis().toString}")
  val cnt2 = r.count() //have to use count to trigger the get rdd lazy val
  println(s"cnt2 = $cnt2")
  println(s"time stamp 1 -> ${System.currentTimeMillis().toString}")


}
