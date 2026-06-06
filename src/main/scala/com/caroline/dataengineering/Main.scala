package com.caroline.dataengineering

import com.caroline.dataengineering.gold.GoldAggregation

object Main {
  def main(args: Array[String]): Unit = {
    GoldAggregation.aggregate(
      inputPath  = "C:/Users/ana/Codes/spark-ecommerce-pipeline/spark-ecommerce-pipeline/data/silver/transactions",
      outputPath = "C:/Users/ana/Codes/spark-ecommerce-pipeline/spark-ecommerce-pipeline/data/gold/merchants"
    )
  }
}