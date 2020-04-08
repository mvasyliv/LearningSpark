
//val listCountries = countryDF.select('ID).distinct().rdd.map(r => r(0)).collect()
//println(s"list countries: $listCountries")
//
//listCountries.par.foreach(i => {
//  countryDF.filter('ID.equalTo(i)).createTempView(s"tmp_table_country_$i")
//})

package spark

import org.apache.spark.sql.{SparkSession}

object DynamicTempTable extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("Mapper")
    .getOrCreate()

  import spark.implicits._

  case class Country(
                     ID: Int,
                     name: String
                    )

  case class CountryData(
                      ID: Int,
                      capital: String,
                      population: Long
                    )

  case class CountryTable(
                      ID: Int,
                      name: String,
                      nameTable: String
                    )

  val countryDF = Seq(
    Country(1, "c1"),
    Country(2, "c2"),
    Country(3, "c3"),
    Country(4, "c4"),
    Country(5, "c5"),
    Country(6, "c6"),
    Country(7, "c7"),
    Country(8, "c8"),
    Country(9, "c9"),
    Country(10, "c10")
  ).toDF()


  val countryData = Seq(
    CountryData(1, "cp1", 11),
    CountryData(2, "cp2", 22),
    CountryData(3, "cp3", 33),
    CountryData(4, "cp4", 44),
    CountryData(5, "cp5", 55),
    CountryData(6, "cp6", 66),
    CountryData(7, "cp7", 77),
    CountryData(8, "cp8", 88),
    CountryData(9, "cp9", 99),
    CountryData(10, "cp10", 1010)
  ).toDF()

  import scala.collection.mutable.ListBuffer
  var tableToCountry = new ListBuffer[CountryTable]()

  countryDF.collect().foreach(i => {

    val nameTempTable = s"${i.getAs[String]("name")}_temp_table"
    val countryId = i.getAs[Int]("ID")
    val countryName = i.getAs[String]("name")

    countryData.filter('ID.equalTo(countryId)).createOrReplaceTempView(nameTempTable)

    tableToCountry += CountryTable(countryId, countryName, nameTempTable)

  })

  val tcDF = tableToCountry.toDF()
  tcDF.show(false)
//  +---+----+--------------+
//  |ID |name|nameTable     |
//  +---+----+--------------+
//  |1  |c1  |c1_temp_table |
//  |2  |c2  |c2_temp_table |
//  |3  |c3  |c3_temp_table |
//  |4  |c4  |c4_temp_table |
//  |5  |c5  |c5_temp_table |
//  |6  |c6  |c6_temp_table |
//  |7  |c7  |c7_temp_table |
//  |8  |c8  |c8_temp_table |
//  |9  |c9  |c9_temp_table |
//  |10 |c10 |c10_temp_table|
//  +---+----+--------------+

  tcDF.createOrReplaceTempView("table_to_country")

  spark.table("table_to_country").show(false)
//  +---+----+--------------+
//  |ID |name|nameTable     |
//  +---+----+--------------+
//  |1  |c1  |c1_temp_table |
//  |2  |c2  |c2_temp_table |
//  |3  |c3  |c3_temp_table |
//  |4  |c4  |c4_temp_table |
//  |5  |c5  |c5_temp_table |
//  |6  |c6  |c6_temp_table |
//  |7  |c7  |c7_temp_table |
//  |8  |c8  |c8_temp_table |
//  |9  |c9  |c9_temp_table |
//  |10 |c10 |c10_temp_table|
//  +---+----+--------------+


  println(s"~~~~> check result for table ${tcDF.select('nameTable).take(1)(0).mkString}")
  spark.table(tcDF.select('nameTable).take(1)(0).mkString).show(false)

//  ~~~~> check result for table c1_temp_table
//  +---+-------+----------+
//  |ID |capital|population|
//  +---+-------+----------+
//  |1  |cp1    |11        |
//  +---+-------+----------+




  countryDF.select('ID, 'name).collect.foreach(row => {
      val df = countryDF.filter('ID.equalTo(row.getAs[Int]("ID")))
    df.show(false)
      countryDF.filter('ID.equalTo(row.getAs[Int]("ID"))).createOrReplaceTempView(s"${row.getAs[String]("name")}_temp_table")
  })



}
