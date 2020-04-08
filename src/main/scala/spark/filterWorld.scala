    package spark

    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.functions.{col}

    object filterWorld extends App {

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

      personDF.show(false)
    //  +---+---------+--------+-----------+-------+
    //  |ID |firstName|lastName|description|comment|
    //  +---+---------+--------+-----------+-------+
    //  |1  |FN1      |LN1     |TEST       |scala  |
    //  |2  |FN2      |LN2     |develop    |spark  |
    //  |3  |FN3      |LN3     |test       |sql    |
    //  |4  |FN4      |LN4     |develop    |java   |
    //  |5  |FN5      |LN5     |test       |c#     |
    //  |6  |FN6      |LN6     |architect  |python |
    //  |7  |FN7      |LN7     |test       |spark  |
    //  |8  |FN8      |LN8     |architect  |scala  |
    //  |9  |FN9      |LN9     |qa         |hql    |
    //  |10 |FN10     |LN10    |manager    |haskell|
    //  +---+---------+--------+-----------+-------+
    //
      val fltr = !col("description").like("%e%") && !col("comment").like("%s%")

      val res = personDF.filter(fltr)
      res.show(false)
    //  +---+---------+--------+-----------+-------+
    //  |ID |firstName|lastName|description|comment|
    //  +---+---------+--------+-----------+-------+
    //  |9  |FN9      |LN9     |qa         |hql    |
    //  +---+---------+--------+-----------+-------+




    }
