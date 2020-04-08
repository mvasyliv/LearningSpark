package spark

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{col, column, sum}

object DynamicColumnSelection extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("DataFrame-example")
    .getOrCreate()

  import spark.implicits._

  case class c1(
     country: String,
     st_buy: Double,
     nd_buy: Double
  )

  case class c2(
     country: String,
     percent: Double
  )

val df1 = Seq(
  c1("UA", 2, 4),
  c1("PL", 3, 6),
  c1("GB", 4, 8)
  )
  .toDF()

  df1.show(false)
//  +-------+------+------+
//  |country|st_buy|nd_buy|
//  +-------+------+------+
//  |UA     |2.0   |4.0   |
//  |PL     |3.0   |6.0   |
//  |GB     |4.0   |8.0   |
//  +-------+------+------+

  val df2 = Seq(
    c2("UA", 2.21),
    c2("PL", 3.26)
  )
    .toDF()
  df2.show(false)
//  +-------+-------+
//  |country|percent|
//  +-------+-------+
//  |UA     |2.21   |
//  |PL     |3.26   |
//  +-------+-------+


  // Inner Join
  val df = df1.join(df2, df1.col("country") === df2.col("country"), "inner")
      .select(
        df1.col("country"),
        df1.col("st_buy"),
        df1.col("nd_buy"),
        df2.col("percent")
      )
  df.show(false)
//  +-------+------+------+-------+
//  |country|st_buy|nd_buy|percent|
//  +-------+------+------+-------+
//  |UA     |2.0   |4.0   |2.21   |
//  |PL     |3.0   |6.0   |3.26   |
//  +-------+------+------+-------+


  val res1DF = df.withColumn("st_buy_percent", 'st_buy/'percent)
    .withColumn("nd_buy_percent", 'nd_buy/'percent)

  res1DF.show(false)
//  +-------+------+------+-------+------------------+------------------+
//  |country|st_buy|nd_buy|percent|st_buy_percent    |nd_buy_percent    |
//  +-------+------+------+-------+------------------+------------------+
//  |UA     |2.0   |4.0   |2.21   |0.9049773755656109|1.8099547511312217|
//  |PL     |3.0   |6.0   |3.26   |0.9202453987730062|1.8404907975460123|
//  +-------+------+------+-------+------------------+------------------+


  // GroupBy + sum
  val data = Seq(
    c1("UA", 2, 4),
    c1("PL", 3, 6),
    c1("UA", 5, 10),
    c1("PL", 6, 12),
    c1("GB", 4, 8)
  )
    .toDF()

  val resGroupByDF = data
    .groupBy("country")
    .agg(sum("st_buy").alias("sum_st_buy")
    ,sum("nd_buy").alias("sum_nd_buy"))

  resGroupByDF.show(false)
//  +-------+----------+----------+
//  |country|sum_st_buy|sum_nd_buy|
//  +-------+----------+----------+
//  |UA     |7.0       |14.0      |
//  |PL     |9.0       |18.0      |
//  |GB     |4.0       |8.0       |
//  +-------+----------+----------+


  val resGroupByDF1 = data.groupBy($"country").sum()
  resGroupByDF1.show(false)
//  +-------+-----------+-----------+
//  |country|sum(st_buy)|sum(nd_buy)|
//  +-------+-----------+-----------+
//  |UA     |7.0        |14.0       |
//  |PL     |9.0        |18.0       |
//  |GB     |4.0        |8.0        |
//  +-------+-----------+-----------+


  val exprs = data.columns.map(sum(_))
  val resGroupByDF2 = data.groupBy($"country").agg(exprs.head, exprs.tail: _*)
  resGroupByDF2.show(false)
//  +-------+------------+-----------+-----------+
//  |country|sum(country)|sum(st_buy)|sum(nd_buy)|
//  +-------+------------+-----------+-----------+
//  |UA     |null        |7.0        |14.0       |
//  |PL     |null        |9.0        |18.0       |
//  |GB     |null        |4.0        |8.0        |
//  +-------+------------+-----------+-----------+

  val exprs3 = List("st_buy", "nd_buy").map(sum(_))
  val resGroupByDF3 = data.groupBy($"country").agg(exprs3.head, exprs3.tail: _*)
  resGroupByDF3.show(false)
//  +-------+-----------+-----------+
//  |country|sum(st_buy)|sum(nd_buy)|
//  +-------+-----------+-----------+
//  |UA     |7.0        |14.0       |
//  |PL     |9.0        |18.0       |
//  |GB     |4.0        |8.0        |
//  +-------+-----------+-----------+


  val exprs4 = data.columns.toList.filter(_ != "country").map(sum(_))
  val resGroupByDF4 = data.groupBy($"country").agg(exprs4.head, exprs4.tail: _*)
  resGroupByDF4.show(false)

//  +-------+-----------+-----------+
//  |country|sum(st_buy)|sum(nd_buy)|
//  +-------+-----------+-----------+
//  |UA     |7.0        |14.0       |
//  |PL     |9.0        |18.0       |
//  |GB     |4.0        |8.0        |
//  +-------+-----------+-----------+

  // Select
  val cols = data.columns.toSeq
  val selectDF1 = data.select(cols.head, cols.tail:_*)
  selectDF1.show(false)
//  +-------+------+------+
//  |country|st_buy|nd_buy|
//  +-------+------+------+
//  |UA     |2.0   |4.0   |
//  |PL     |3.0   |6.0   |
//  |UA     |5.0   |10.0  |
//  |PL     |6.0   |12.0  |
//  |GB     |4.0   |8.0   |
//  +-------+------+------+
}

