package spark

import org.apache.spark.sql.SparkSession

object DynamicNameDataFrame extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("Mapper")
    .getOrCreate()

  import spark.implicits._

  case class Person(
                     ID: Int,
                     firstName: String,
                     lastName: String,
                     description: String,
                     comment: String
                   )


  val personDF = Seq(
    Person(1, "FN1", "LN1", "TEST", "scala"),
    Person(2, "FN2", "LN2", "develop", "spark"),
    Person(3, "FN3", "LN3", "test", "sql"),
    Person(4, "FN4", "LN4", "develop", "java"),
    Person(5, "FN5", "LN5", "test", "c#"),
    Person(6, "FN6", "LN6", "architect", "python"),
    Person(7, "FN7", "LN7", "test", "spark"),
    Person(8, "FN8", "LN8", "architect", "scala"),
    Person(9, "FN9", "LN9", "qa", "hql"),
    Person(10, "FN10", "LN10", "manager", "haskell")
  ).toDF()

  // dynamic DataFrame name

  val df = personDF

  val stringVariable = "personData"

  // assign name to dataframe
  val namedDataFrames = Map("df_" + stringVariable -> df)

  // get dataframe by name // "df_part1324"
  namedDataFrames(s"df_$stringVariable").show(false)


}
