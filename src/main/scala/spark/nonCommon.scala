package spark

import org.apache.spark.sql.SparkSession

object nonCommon extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("DataFrame-example")
    .getOrCreate()

  import spark.implicits._

  val df = Seq(
    (1,"st" ,"aaa"),
  (1,"st", "bbb"),
  (2,"ac" ,"aaa"),
  (2,"ac", "ccc"),
  (3,"ac", "ggg"),
  (3,"ds" ,"aaa"),
  (3,"ds", "ggg"),
  (3,"ds", "mmm")
  ).toDF("id","code" ,"prod")


  df.show()

  val pairId = Seq(("st" ,"ac"), ("st" ,"ac"),("st","ds" ),("st","ds" ),("ac" ,"ds" ))
    .toDF("idSorce","idTarget" )

  pairId.show(false)

      val df1 = df
        .filter('prod.notEqual("aaa"))

      val resDF = pairId
        .join(df1, pairId.col("idTarget") === df1.col("code"), "inner")
        .select(
            pairId.col("idSorce"),
            pairId.col("idTarget"),
            df1.col("prod")
          )
        .dropDuplicates()
        .orderBy('prod)

      resDF.show(false)
    //  +-------+--------+----+
    //  |idSorce|idTarget|prod|
    //  +-------+--------+----+
    //  |st     |ac      |ccc |
    //  |st     |ac      |ggg |
    //  |ac     |ds      |ggg |
    //  |st     |ds      |ggg |
    //  |st     |ds      |mmm |
    //  |ac     |ds      |mmm |
    //  +-------+--------+----+

}
