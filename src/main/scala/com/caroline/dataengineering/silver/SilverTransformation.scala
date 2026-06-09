package com.caroline.dataengineering.silver

import com.caroline.dataengineering.config.SparkConfig
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object SilverTransformation {

  def transform(inputPath: String, outputPath: String): Unit = {
    val spark = SparkConfig.createSparkSession("Silver Transformation")

    val bronzeDF = spark.read.parquet(inputPath)

    val silverDF = bronzeDF
      .withColumn("amount",
        (col("value_cents").cast("long") / 100).cast("decimal(15,2)"))
      .withColumn("transaction_datetime",
        to_timestamp(col("datetime"), "yyyyMMddHHmmss"))
      .withColumn("installments_count",
        col("installments").cast("int"))
      .withColumn("payment_description",
        when(col("payment_type") === "01", "debit")
          .when(col("payment_type") === "02", "credit")
          .when(col("payment_type") === "03", "pix")
          .otherwise("unknown"))
      .withColumn("status_description",
        when(col("status") === "00", "approved")
          .when(col("status") === "01", "denied")
          .when(col("status") === "02", "cancelled")
          .otherwise("unknown"))
      .drop("value_cents", "datetime", "installments", "payment_type", "status")
      .withColumn("year", year(col("transaction_datetime")))
      .withColumn("month", month(col("transaction_datetime")))

    silverDF
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "month")
      .parquet(outputPath)

    println(s"Silver transformation complete. Records: ${silverDF.count()}")
  }
}