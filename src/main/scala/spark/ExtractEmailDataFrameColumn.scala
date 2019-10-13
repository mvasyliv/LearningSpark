package spark

import org.apache.spark.sql.SparkSession

object ExtractEmailDataFrameColumn extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("DataFrame-example")
    .getOrCreate()

  val reg = """(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}\b""".r

  val reg1 = """(?:[a-z0-9!#$%&'*+/=?^_``{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_``{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])""".r

  val reg2 = "\\S+@\\S+".r

  val reg3 = """[a-zA-Z0-9._-][a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,3}""".r
  import spark.implicits._
  val df = List("vmv_gaz@yahoo.com,vasyliv@gmail.com ad@gmail.com-rrrrrrr user-name@domain.co.in adr@gmail.comrrrrrrr ttttttttt", "test", "test@yahoo.com").toDF("col1")
  df.show(false)

  val ds1 = df.as[String]
  ds1.show(false)
  val res = ds1.flatMap{x1 => reg3.findAllIn(x1)}.toDF("col2")
  res.show(false)

}
