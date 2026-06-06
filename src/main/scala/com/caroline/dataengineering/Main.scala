package com.caroline.dataengineering

import com.caroline.dataengineering.silver.SilverTransformation

object Main {
  def main(args: Array[String]): Unit = {
    SilverTransformation.transform(
      inputPath  = "C:/Users/ana/Codes/spark-ecommerce-pipeline/spark-ecommerce-pipeline/data/bronze/transactions",
      outputPath = "C:/Users/ana/Codes/spark-ecommerce-pipeline/spark-ecommerce-pipeline/data/silver/transactions"
    )
  }
}