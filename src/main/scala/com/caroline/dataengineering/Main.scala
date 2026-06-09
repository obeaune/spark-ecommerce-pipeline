package com.caroline.dataengineering

import com.caroline.dataengineering.bronze.BronzeIngestion
import com.caroline.dataengineering.silver.SilverTransformation
import com.caroline.dataengineering.gold.GoldAggregation
import com.caroline.dataengineering.config.{AppConfig, SparkConfig}
import com.caroline.dataengineering.bronze.DataValidator

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkConfig.createSparkSession("Ecommerce Pipeline")
    val basePath = AppConfig.basePath
    val inputFile = if (args.nonEmpty) args(0) else s"$basePath/input/transactions_20260605.txt"

    try {
      println("=== Starting Ecommerce Pipeline ===")

      println("\n[1/3] Bronze ingestion...")
      BronzeIngestion.ingest(
        inputPath = inputFile,
        outputPath = s"$basePath/bronze/transactions"
      )

      println("\n[1.5/3] Data validation...")
      val bronzeDF = spark.read.parquet(s"$basePath/bronze/transactions")
      val validationResult = DataValidator.validate(bronzeDF)
      println(s"  Total records:   ${validationResult.totalRecords}")
      println(s"  Valid records:   ${validationResult.validRecords}")
      println(s"  Invalid records: ${validationResult.invalidRecords}")
      println(s"  Zero values:     ${validationResult.zeroValueRecords}")
      println(s"  Invalid dates:   ${validationResult.invalidDateRecords}")

      val validDF = DataValidator.filterValid(bronzeDF)
      validDF.write.mode(org.apache.spark.sql.SaveMode.Overwrite)
        .parquet(s"$basePath/bronze/transactions_validated")

      println("\n[2/3] Silver transformation...")
      SilverTransformation.transform(
        inputPath = s"$basePath/bronze/transactions",
        outputPath = s"$basePath/silver/transactions"
      )

      println("\n[3/3] Gold aggregation...")
      GoldAggregation.aggregate(
        inputPath = s"$basePath/silver/transactions",
        outputPath = s"$basePath/gold/merchants"
      )

      println("\n=== Pipeline completed successfully! ===")

    } catch {
      case e: Exception =>
        println(s"\n[ERROR] Pipeline failed: ${e.getMessage}")
        throw e
    } finally {
      spark.stop()
    }
  }
}