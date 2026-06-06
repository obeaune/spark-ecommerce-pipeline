package com.caroline.dataengineering.config

import org.apache.spark.sql.SparkSession

object SparkConfig {
  def createSparkSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }
}
