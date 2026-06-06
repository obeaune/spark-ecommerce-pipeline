package com.caroline.dataengineering.gold

import com.caroline.dataengineering.config.SparkConfig
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object GoldAggregation {

  def aggregate(inputPath: String, outputPath: String): Unit = {
    val spark = SparkConfig.createSparkSession("Gold Aggregation")

    val silverDF = spark.read.parquet(inputPath)

    val goldDF = silverDF
      .groupBy("merchant_id")
      .agg(
        count("transaction_id").as("total_transactions"),
        sum("amount").as("total_amount"),
        avg("amount").as("avg_ticket"),
        sum(when(col("status_description") === "approved", 1).otherwise(0)).as("approved_count"),
        sum(when(col("status_description") === "denied", 1).otherwise(0)).as("denied_count"),
        sum(when(col("status_description") === "cancelled", 1).otherwise(0)).as("cancelled_count"),
        sum(when(col("payment_description") === "debit", col("amount")).otherwise(0)).as("debit_amount"),
        sum(when(col("payment_description") === "credit", col("amount")).otherwise(0)).as("credit_amount"),
        sum(when(col("payment_description") === "pix", col("amount")).otherwise(0)).as("pix_amount")
      )
      .orderBy(desc("total_amount"))

    goldDF
      .write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath)

    println(s"Gold aggregation complete. Merchants: ${goldDF.count()}")
    goldDF.show(10, truncate = false)

    spark.stop()
  }
}