    package spark

    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.functions.{explode, col}
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

      // explode
      val explodeDF = df
        .withColumn("ipExplode", explode(col("IP")))
          .select('user, 'ipExplode)

      explodeDF.show(50, false)
    //      +----+---------+
    //      |user|ipExplode|
    //      +----+---------+
    //      |1   |10.0.0.1 |
    //      |1   |10.0.0.2 |
    //      |1   |10.0.0.3 |
    //      |1   |10.0.0.4 |
    //      |2   |10.0.0.1 |
    //      |2   |10.0.0.2 |
    //      |2   |10.0.0.3 |
    //      |2   |10.0.0.4 |
    //      |3   |10.0.0.1 |
    //      |3   |10.0.0.2 |
    //      |3   |10.0.0.3 |
    //      |3   |10.0.0.4 |
    //      |4   |10.0.0.1 |
    //      |4   |10.0.0.2 |
    //      |4   |10.0.0.3 |
    //      |4   |10.0.0.4 |
    //      |5   |10.0.0.1 |
    //      |5   |10.0.0.2 |
    //      |5   |10.0.0.3 |
    //      |5   |10.0.0.4 |
    //      |6   |10.0.0.1 |
    //      |6   |10.0.0.2 |
    //      |6   |10.0.0.3 |
    //      |6   |10.0.0.4 |
    //      +----+---------+

      
    }


