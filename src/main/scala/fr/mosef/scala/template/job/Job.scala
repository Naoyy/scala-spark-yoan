package fr.mosef.scala.template.job

import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.writer.Writer
import org.apache.spark.sql.DataFrame
trait Job {
  val reader: Reader
  val processor: Processor
  val writer: Writer
  val src_path: String
  val dst_path: String
  val inputDF: DataFrame
  val SEPA: String
  val processedDF: DataFrame
  /*
  val data: Seq[(Int, String, Int)]
  val df: DataFrame*/
}
