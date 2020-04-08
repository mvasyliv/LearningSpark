package spark

import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.functions._

object CreateColumnInDF extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("DataFrame-example")
    .getOrCreate()

  import spark.implicits._

  val input = Seq(
    ("1 February"),
    ("n"),
    ("c"),
    ("b"),
    ("2 February"),
    ("h"),
    ("w"),
    ("e"),
    ("3 February"),
    ("y"),
    ("s"),
    ("j")
  ).toDF("lines")

  input.show()




  val df = Seq(
    (1L, "Ukraine"),
    (2L, "Germany")
  ).toDF("id", "name")

  df.show(false)

  df.printSchema()



  val df_data = Seq(
    ("G1","I1","col1_r1", "col2_r1","col3_r1"),
    ("G1","I2","col1_r2", "col2_r2","col3_r3")
  ).toDF("group","industry_id","col1","col2","col3")
    .withColumn("group", $"group".cast(StringType))
    .withColumn("industry_id", $"industry_id".cast(StringType))
    .withColumn("col1", $"col1".cast(StringType))
    .withColumn("col2", $"col2".cast(StringType))
    .withColumn("col3", $"col3".cast(StringType))

df_data.show(false)

  val df_cols = Seq(
    ("1", "usa", Seq("col1","col2","col3")),
    ("2", "ind", Seq("col1","col2"))
  ).toDF("id","name","list_of_colums")
    .withColumn("id", $"id".cast(IntegerType))
    .withColumn("name", $"name".cast(StringType))


  df_cols.show(false)

  val lcArray = df_cols.filter(col("id").equalTo(1))
    .select('list_of_colums)
      .first()(0)
//      .first().map(x => x.get(0)).mkString(",")


  println(s"lcArray = $lcArray")

//  val r1 = lcArray.

//  import org.apache.spark.sql.functions.concat_ws

//  val resDF = df_data.select(concat_ws(",", s"$lcArray"))
//  resDF.show(false)

//  CREATE TABLE if not exists
//    dbname.tablename_csv (
//      id STRING,
//      location STRING,
//      city STRING,
//      country STRING)
//  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE ;

//  CREATE TABLE  if not exists
//    dbname.tablename_orc (
//      id String,
//      location STRING,
//      country String
//        PARTITIONED BY (city string)
//        CLUSTERED BY (country)
//        into 4 buckets ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
//        STORED AS ORCFILE tblproperties("orc.compress"="SNAPPY");

  var query=spark.sql("id,location,city,country from dbname.tablename_csv")
  query.write.insertInto("dbname.tablename_orc")

}
