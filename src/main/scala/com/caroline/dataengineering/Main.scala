package com.caroline.dataengineering

import com.caroline.dataengineering.config.SparkConfig

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkConfig.createSparkSession("Ecommerce Pipeline.")

    println("Spark session started!")
    println(s"Spark version: ${spark.version}")

    spark.stop()
  }
}