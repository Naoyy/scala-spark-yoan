package fr.mosef.scala.template.reader.impl

import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.reader.Reader
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}

class ReaderImpl(sparkSession: SparkSession) extends Reader {
  def read(format: String, options: Map[String, String], path: String): DataFrame = {
    sparkSession
      .read
      .options(options)
      .format(format)
      .load(path)
  }

  def read(path: String,sep:String): DataFrame = {
    sparkSession
      .read
      .option("sep",sep)
      .option("inferSchema", "true")
      .option("header", "true")
      .format("csv")
      .load(path)
  }

  def read(): DataFrame = {
    sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], StructType(Seq.empty[StructField]))
  }
}
