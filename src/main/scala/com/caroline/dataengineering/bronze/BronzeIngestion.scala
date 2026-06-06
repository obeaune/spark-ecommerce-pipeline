package com.caroline.dataengineering.bronze

import com.caroline.dataengineering.config.SparkConfig
import org.apache.spark.sql.SaveMode

object BronzeIngestion {

  def ingest(inputPath: String, outputPath: String): Unit = {
    val spark = SparkConfig.createSparkSession("Bronze Ingestion")

    import spark.implicits._

    val rawLines = spark.read.textFile(inputPath)

    val header  = rawLines.filter(_.startsWith("00"))
    val details = rawLines.filter(_.startsWith("01"))
    val trailer = rawLines.filter(_.startsWith("99"))

    val bronzeDF = details
      .map(line => (
        line.substring(0, 2),   // record_type
        line.substring(2, 18),  // transaction_id
        line.substring(18, 28), // merchant_id
        line.substring(28, 40), // value_cents
        line.substring(40, 54), // datetime
        line.substring(54, 56), // payment_type
        line.substring(56, 58), // installments
        line.substring(58, 60)  // status
      ))
      .toDF(
        "record_type",
        "transaction_id",
        "merchant_id",
        "value_cents",
        "datetime",
        "payment_type",
        "installments",
        "status"
      )

    bronzeDF
      .write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath)

    println(s"Bronze ingestion complete. Records: ${bronzeDF.count()}")

    spark.stop()
  }
}