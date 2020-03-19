package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GroupListValueToColumn extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("Mapper")
    .getOrCreate()

  case class Customer(
     cust_id: Int,
     addresstype: String
  )

  import spark.implicits._

  val source = Seq(
    Customer(300312008, "credit_card"),
    Customer(300312008, "to"),
    Customer(300312008, "from"),
    Customer(300312009, "to"),
    Customer(300312009, "from"),
    Customer(300312010, "to"),
    Customer(300312010, "credit_card"),
    Customer(300312010, "from")
  ).toDF()

  val res = source.groupBy("cust_id").agg(collect_list("addresstype"))
    .withColumnRenamed("collect_set(addresstype)", "addresstype")

  res.show(false)
//  +---------+-------------------------+
//  |cust_id  |collect_list(addresstype)|
//  +---------+-------------------------+
//  |300312010|[to, credit_card, from]  |
//  |300312008|[credit_card, to, from]  |
//  |300312009|[to, from]               |
//  +---------+-------------------------+

  val res1 = source.groupBy("cust_id").agg(collect_set("addresstype"))
    .withColumnRenamed("collect_set(addresstype)", "addresstype")

  res1.show(false)

//  +---------+------------------------+
//  |cust_id  |collect_set(addresstype)|
//  +---------+------------------------+
//  |300312010|[from, to, credit_card] |
//  |300312008|[from, to, credit_card] |
//  |300312009|[from, to]              |
//  +---------+------------------------+
}
