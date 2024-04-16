package fr.mosef.scala.template.writer

import fr.mosef.scala.template.reader.Reader
import org.apache.spark.sql.DataFrame
import java.util.Properties
import java.io.FileInputStream
class WriterImpl(propertiesFile: String) extends Writer {

  val props = new Properties()
  props.load(new FileInputStream(propertiesFile))

  val csvFormat = props.getProperty("writer.format.csv")
  val parquetFormat = props.getProperty("writer.format.parquet")

  def write(df: DataFrame, mode: String = "overwrite", path: String): Unit = {
    df
      .write
      .option("header", "true")
      .mode(mode)
      .format(csvFormat)
      .save(path)
  }

  def writeParquet(df: DataFrame, mode: String = "overwrite", path: String): Unit = {
    df
      .write
      .mode(mode)
      .format(parquetFormat)
      .save(path)
    }
}