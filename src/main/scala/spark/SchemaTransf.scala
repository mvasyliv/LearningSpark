    package spark

    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.functions.{col, concat_ws, lit, substring}
    import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType, DateType}

    object SchemaTransf extends App {

      val spark = SparkSession.builder()
        .master("local")
        .appName("Spark SQL")
        .getOrCreate()

      import spark.implicits._


      case class source(
       Col1:   String,
       Col2:   String,
       Col3:   String
     )

      val sourceDF = Seq(
        source("1", "Car", "01012020"),
        source("2", "Dog", "15032018"),
        source("3", "Mouse", "22022019"))
        .toDF()

      sourceDF.show(false)
    //  +----+-----+--------+
    //  |Col1|Col2 |Col3    |
    //  +----+-----+--------+
    //  |1   |Car  |01012020|
    //  |2   |Dog  |15032018|
    //  |3   |Mouse|22022019|
    //  +----+-----+--------+




      val r1 = sourceDF.withColumn("c3",
        concat_ws("-",
          substring(col("Col3"), 5,4),
          substring(col("Col3"), 3,2),
          substring(col("Col3"), 1,2))
        )
          .select(
            'Col1.alias("AnimalID").cast(IntegerType),
            'Col2.alias("Animal").cast(StringType),
            'c3.alias("PurchaseDate").cast(TimestampType)
          )


      r1.show(false)
    //  +--------+------+-------------------+
    //  |AnimalID|Animal|PurchaseDate       |
    //  +--------+------+-------------------+
    //  |1       |Car   |2020-01-01 00:00:00|
    //  |2       |Dog   |2018-03-15 00:00:00|
    //  |3       |Mouse |2019-02-22 00:00:00|
    //  +--------+------+-------------------+

      r1.printSchema()
    //  root
    //  |-- AnimalID: integer (nullable = true)
    //  |-- Animal: string (nullable = true)
    //  |-- PurchaseDate: timestamp (nullable = true)




    }
