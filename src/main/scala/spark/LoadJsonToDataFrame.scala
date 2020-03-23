    package spark

    import org.apache.spark.sql.SparkSession

    /**
     * Load json string to DataFrame
     */
    object LoadJsonToDataFrame extends App {

      val sampleJson = """[
                         |  {"user":1, "IP" :["10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"]},
                         |  {"user":2, "IP" :["10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"]},
                         |  {"user":3, "IP" :["10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"]},
                         |  {"user":4, "IP" :["10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"]},
                         |  {"user":5, "IP" :["10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"]},
                         |  {"user":6, "IP" :["10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"]}
                         |]""".stripMargin

      val spark = SparkSession.builder()
        .master("local")
        .appName("DataFrame-example")
        .getOrCreate()

      import spark.implicits._

      val df1 = spark.sparkContext.parallelize(Seq(sampleJson)).toDF("value")
      df1.show(false)
      val rdd = df1.rdd.map(_.getString(0))

      val ds = rdd.toDS
      val df = spark.read.json(ds)

      df.show(false)
    //  +----------------------------------------+----+
    //  |IP                                      |user|
    //  +----------------------------------------+----+
    //  |[10.0.0.1, 10.0.0.2, 10.0.0.3, 10.0.0.4]|1   |
    //  |[10.0.0.1, 10.0.0.2, 10.0.0.3, 10.0.0.4]|2   |
    //  |[10.0.0.1, 10.0.0.2, 10.0.0.3, 10.0.0.4]|3   |
    //  |[10.0.0.1, 10.0.0.2, 10.0.0.3, 10.0.0.4]|4   |
    //  |[10.0.0.1, 10.0.0.2, 10.0.0.3, 10.0.0.4]|5   |
    //  |[10.0.0.1, 10.0.0.2, 10.0.0.3, 10.0.0.4]|6   |
    //  +----------------------------------------+----+

      df.printSchema()
    //  root
    //  |-- IP: array (nullable = true)
    //  |    |-- element: string (containsNull = true)
    //  |-- user: long (nullable = true)

    }


