package com.caroline.dataengineering

import com.caroline.dataengineering.bronze.BronzeIngestion
import com.caroline.dataengineering.silver.SilverTransformation
import com.caroline.dataengineering.gold.GoldAggregation
import com.caroline.dataengineering.config.{AppConfig, SparkConfig}

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkConfig.createSparkSession("Ecommerce Pipeline")
    val basePath = AppConfig.basePath
    val inputFile = if (args.nonEmpty) args(0) else s"$basePath/input/transactions_20260605.txt"

    println("=== Starting Ecommerce Pipeline ===")

    println("\n[1/3] Bronze ingestion...")
    BronzeIngestion.ingest(
      inputPath  = inputFile,
      outputPath = s"$basePath/bronze/transactions"
    )

    println("\n[2/3] Silver transformation...")
    SilverTransformation.transform(
      inputPath  = s"$basePath/bronze/transactions",
      outputPath = s"$basePath/silver/transactions"
    )

    println("\n[3/3] Gold aggregation...")
    GoldAggregation.aggregate(
      inputPath  = s"$basePath/silver/transactions",
      outputPath = s"$basePath/gold/merchants"
    )

    println("\n=== Pipeline completed successfully! ===")
    spark.stop()
  }
}