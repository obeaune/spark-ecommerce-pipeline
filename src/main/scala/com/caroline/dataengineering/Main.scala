package com.caroline.dataengineering

import com.caroline.dataengineering.bronze.BronzeIngestion

object Main {
  def main(args: Array[String]): Unit = {
    BronzeIngestion.ingest(
      inputPath  = "C:/Users/ana/Codes/spark-ecommerce-pipeline/spark-ecommerce-pipeline/data/input/transactions_20260605.txt",
      outputPath = "C:/Users/ana/Codes/spark-ecommerce-pipeline/spark-ecommerce-pipeline/data/bronze/transactions"
    )
  }
}