package fr.mosef.scala.template.processor.impl

import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


class ProcessorImpl() extends Processor {

  def process(inputDF: DataFrame): DataFrame = {
    val sumDF = inputDF.groupBy("group_key").agg(sum("field1").alias("sum_field1"))

    val avgDF = inputDF.groupBy("group_key").agg(avg("field1").alias("avg_field1"))

    val resultDF = sumDF.join(avgDF, Seq("group_key"), "inner")

    val countDF = inputDF.groupBy("group_key").agg(count("field1").alias("count_field1"))

    val finalDF = resultDF.join(countDF, Seq("group_key"), "inner")

    finalDF
    }
}