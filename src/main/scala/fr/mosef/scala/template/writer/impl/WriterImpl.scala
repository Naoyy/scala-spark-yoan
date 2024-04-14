package fr.mosef.scala.template.writer

import fr.mosef.scala.template.reader.Reader
import org.apache.spark.sql.DataFrame
class WriterImpl() extends Writer {

  def write(df: DataFrame, mode: String = "overwrite", path: String): Unit = {
    df
      .write
      .option("header", "true")
      .mode(mode)
      .csv(path)
  }

  def writeParquet(df: DataFrame, mode: String = "overwrite", path: String): Unit = {
    df
      .write
      .mode(mode)
      .parquet(path)
    }
}
