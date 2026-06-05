package com.caroline.dataengineering

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Ecommerce Pipeline")
      .master("local[*]")
      .getOrCreate()

    println("Spark session iniciada!")
    println(s"Versão do Spark: ${spark.version}")

    spark.stop()
  }
}