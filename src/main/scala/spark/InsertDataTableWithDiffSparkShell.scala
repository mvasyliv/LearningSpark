package spark

import org.apache.spark.sql.SparkSession

object InsertDataTableWithDiffSparkShell {

  def Insert(spark: SparkSession): Unit ={
    import spark.implicits._

    //    1. start first spark shell
    // Create DataFrame
    val l1 = Seq((1, "test 1"), (2, "test 2")).toDF("col1", "col2")
    // Save data to table vmv.t_tmptest
    l1.write.format("parquet").mode("append").saveAsTable("vmv.t_tmptest")

    // Count rows in table
    val df1 = spark.table("vmv.t_tmptest")
    df1.count
    //    Long = 2

    // Add new rows to table
    val l2 = Seq((3, "test 3"), (4, "test 4")).toDF("col1", "col2")
    // Save  DataFrame to table vmv.t_tmptest
    l2.write.format("parquet").mode("append").saveAsTable("vmv.t_tmptest")

    df1.count
    //    res9: Long = 4  df1 show correcty count

    spark.table("vmv.t_tmptest").count
    //    res10: Long = 4 spark table also show correct count

    // Start second spark shell
    // in second spark shell
    val l3 = Seq((5, "test 5"), (6, "test 6")).toDF("col1", "col2")

    l3.write.format("parquet").mode("append").saveAsTable("vmv.t_tmptest")

    spark.table("vmv.t_tmptest").count
    //    res1: Long = 6  count show correct

    //
    // Return to  first spark shell
    spark.table("vmv.t_tmptest").count
    // !!!!!    res10: Long = 4  row count bad.  and we not received msg about new row !!!!!

    // TRY
    spark.catalog.refreshTable("vmv.t_tmptest")

    // then
    spark.table("vmv.t_tmptest").count
    //    res21: Long = 6 receive correct rows in table

  }
}
