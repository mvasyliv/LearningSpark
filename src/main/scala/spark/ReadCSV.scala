package spark

import org.apache.spark.sql.SparkSession

object ReadCSV extends App {
  //  ID,Name,Age,Add,ress,Salary
  //  1,Ross,32,Ah,med,abad,2000
  //  2,Rachel,25,Delhi,1500
  //  3,Chandler,23,Kota,2000
  //  4,Monika,25,Mumbai,6500
  //  5,Mike,27,Bhopal,8500
  //  6,Phoebe,22,MP,4500
  //  7,Joey,24,Indore,10000

  val spark = SparkSession.builder()
    .master("local")
    .appName("DataFrame-example")
    .getOrCreate()

  import spark.implicits._

  val fileCSV = "readcsv.csv"
  val fileFullName = getClass.getResource(s"/$fileCSV").getFile()


    //  1. you need in format csv:
    //  ID,Name,Age,Add,ress,Salary
    //  1,Ross,32,Ah,"med,abad",2000
    //  2,Rachel,25,Delhi,,1500
    //  3,Chandler,23,Kota,,2000
    //  4,Monika,25,Mumbai,,6500
    //  5,Mike,27,Bhopal,,8500
    //  6,Phoebe,22,MP,,4500
    //  7,Joey,24,Indore,,10000

    //  2. read csv:
      val df1 = spark.read.option("header", "true").csv(fileFullName)
      df1.show(false)

    //  3. result


    //    +---+--------+---+------+--------+------+
    //    |ID |Name    |Age|Add   |ress    |Salary|
    //    +---+--------+---+------+--------+------+
    //    |1  |Ross    |32 |Ah    |med,abad|2000  |
    //    |2  |Rachel  |25 |Delhi |null    |1500  |
    //    |3  |Chandler|23 |Kota  |null    |2000  |
    //    |4  |Monika  |25 |Mumbai|null    |6500  |
    //    |5  |Mike    |27 |Bhopal|null    |8500  |
    //    |6  |Phoebe  |22 |MP    |null    |4500  |
    //    |7  |Joey    |24 |Indore|null    |10000 |
    //    +---+--------+---+------+--------+------+


}
