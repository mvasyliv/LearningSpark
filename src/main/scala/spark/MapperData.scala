package spark

import org.apache.spark.sql.SparkSession

object MapperData extends App {

  case class Person(
     ID:Int,
     name:String,
     age:Int,
     numFriends:Int
  )

  def mapper(line:String): Person = {
    val fields = line.split(',')

    Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)

  }

  val spark = SparkSession.builder()
    .master("local")
    .appName("Mapper")
    .getOrCreate()

  val resource = "person-data.csv"
  val fileName = getClass.getResource(s"/$resource").getFile()
  val lines = spark.sparkContext.textFile(fileName)
  val people = lines.map(mapper)

  import spark.implicits._

  val p = people.toDS()
  p.printSchema()

  p.show(false)

}
