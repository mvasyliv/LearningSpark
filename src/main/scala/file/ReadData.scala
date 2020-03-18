package file

import org.apache.spark.sql.{SparkSession, DataFrame}

trait ReadData {

  def read(spark: SparkSession, resource: String, format: String, header: Boolean): DataFrame= {

    val fileName = getClass.getResource(s"/$resource").getFile()

    spark.read.format(format).option("header", header).load(fileName)

  }

}
