    package spark

    import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, types}
    import org.apache.spark.sql.types.{ArrayType, DataType, StringType, StructField, StructType}

    import scala.io.Source

    object DFFieldsWithJson extends App {

      val spark = SparkSession.builder()
        .master("local")
        .appName("DataFrame-example")
        .getOrCreate()

      import spark.implicits._

      case class TestData (
        id:         Int,
        firstName:  String,
        lastName:   String,
        descr:      String
      )

      val dataTestDF = Seq(
        TestData(1, "First Name 1", "Last Name 1", "Description 1"),
        TestData(2, "First Name 2", "Last Name 2", "Description 2"),
        TestData(3, "First Name 3", "Last Name 3", "Description 3")
      ).toDF()

      dataTestDF.show(false)
    //  +---+------------+-----------+-------------+
    //  |id |firstName   |lastName   |descr        |
    //  +---+------------+-----------+-------------+
    //  |1  |First Name 1|Last Name 1|Description 1|
    //  |2  |First Name 2|Last Name 2|Description 2|
    //  |3  |First Name 3|Last Name 3|Description 3|
    //  +---+------------+-----------+-------------+

      val schemaJson =
        """{ "type" : "struct",
          |"fields" : [
          |{
          |    "name" : "id",
          |    "type" : "integer",
          |    "nullable" : true,
          |    "metadata" : { }
          |  },
          |  {
          |    "name" : "firstName",
          |    "type" : "string",
          |    "nullable" : true,
          |    "metadata" : {}
          |  },
          |  {
          |    "name" : "lastName",
          |    "type" : "string",
          |    "nullable" : true,
          |    "metadata" : {}
          |  }
          |  ]}""".stripMargin

      val schemaSource = schemaJson.mkString
      val schemaFromJson = DataType.fromJson(schemaSource).asInstanceOf[StructType]

      println(schemaFromJson)
    //  StructType(StructField(id,IntegerType,true), StructField(firstName,StringType,true), StructField(lastName,StringType,true))


      val cols: List[String] = schemaFromJson.fieldNames.toList
      val col: List[Column] = cols.map(dataTestDF(_))
      val df = dataTestDF.select(col: _*)


      df.printSchema()

    //  root
    //  |-- id: integer (nullable = false)
    //  |-- firstName: string (nullable = true)
    //  |-- lastName: string (nullable = true)

      df.show(false)
    //  +---+------------+-----------+
    //  |id |firstName   |lastName   |
    //  +---+------------+-----------+
    //  |1  |First Name 1|Last Name 1|
    //  |2  |First Name 2|Last Name 2|
    //  |3  |First Name 3|Last Name 3|
    //  +---+------------+-----------+


    }
