package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object RangeTime extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("Mapper")
    .getOrCreate()

  import spark.implicits._

  // Range time every 15 minutes
  val df = Range(1584482400, 1584568800, 900).zip(Range(1584483300, 1584569700, 900))
    .toDF("tsStart", "tsEnd")
    .withColumn("start", from_unixtime(col("tsStart"), "HH:mm"))
    .withColumn("end", from_unixtime(col("tsEnd"), "HH:mm"))
    .select('start, 'end)

  df.show(100, false)

  // df save as table or file
}
