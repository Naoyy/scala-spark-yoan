/*import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.SparkConf

object Main extends App {
  val conf: SparkConf = new SparkConf()
  conf.set("spark.driver.memory","1G")
    .set("spark.testing.memory","2147480000")

  val sparkSession = SparkSession
    .builder
    .master("local[1]")
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  print(sparkSession.sql("SELECT 'A'").show())
}*/

package fr.mosef.scala.template

import fr.mosef.scala.template.job.Job
import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.processor.impl.ProcessorImpl
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.reader.impl.ReaderImpl
import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.writer.Writer
import org.apache.spark.SparkConf
import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{SparkSession, SaveMode}
import fr.mosef.scala.template.writer.WriterImpl

object Main extends App with Job {
  val cliArgs = args
  val MASTER_URL: String = try {
    cliArgs(0)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => "local[1]"
  }
  val SRC_PATH: String = try {
    cliArgs(1)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./src/main/ressources/data.csv"
    }
  }
  val SEPA: String = try {
    cliArgs(2)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      ""
    }
  }
  val DST_PATH: String = try {
    cliArgs(3)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./default/output-writer"
    }
  }

  val conf: SparkConf = new SparkConf()
  conf.set("spark.testing.memory", "471859200")

  val sparkSession = SparkSession
    .builder
    .master(MASTER_URL)
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  sparkSession.sparkContext.hadoopConfiguration.setClass("fs.file.impl",  classOf[ BareLocalFileSystem], classOf[FileSystem])
/*
  val spark = SparkSession.builder()
    .appName("TableHive")
    .enableHiveSupport()
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  //Création du DataFrame avec les données fournies
  val data = Seq(
    (1, "CECILE", 1),
    (1, "CECILE", 300),
    (2, "ALICE", 34),
    (3, "YOAN", 2),
    (3, "YOAN", 34)
  )
  val df = data.toDF("iddugars", "nomdugars", "salairedumec")

  // Stockage des données dans un répertoire local
  val localPath = "./src/main/ressources/table_data"
  df.write.mode(SaveMode.Overwrite).csv(localPath)

  // Création de la table Hive
  val tableName = "my_hive_table"
  df.write.mode(SaveMode.Overwrite).saveAsTable(tableName)

  // Affichage du schéma de la table Hive
  println(spark.sql(s"DESCRIBE $tableName").show())
*/
  val reader: Reader = new ReaderImpl(sparkSession)
  val processor: Processor = new ProcessorImpl()
  val writer: Writer = new WriterImpl()
  val src_path = SRC_PATH
  val dst_path = DST_PATH

  val inputDF = if (src_path.endsWith(".csv")) {
    reader.read(src_path,SEPA)
  } else if (src_path.endsWith(".parquet")) {
    reader.read("parquet", Map.empty, src_path)
  } else {
    throw new IllegalArgumentException("Unsupported file format")
  }
  val processedDF = processor.process(inputDF)

  // Utilisation de write ou writeParquet en fonction du format du fichier source
  if (src_path.endsWith(".csv")) {
    writer.write(processedDF, "overwrite", dst_path)
  } else if (src_path.endsWith(".parquet")) {
    writer.writeParquet(processedDF, "overwrite",dst_path)
    }
}