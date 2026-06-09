package com.caroline.dataengineering.bronze

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DataValidator {

  case class ValidationResult(
                               totalRecords: Long,
                               validRecords: Long,
                               invalidRecords: Long,
                               zeroValueRecords: Long,
                               invalidDateRecords: Long
                             )

  def validate(df: DataFrame): ValidationResult = {
    val total = df.count()

    val zeroValue = df.filter(col("value_cents") === "000000000000").count()

    val invalidDate = df.filter(
      length(col("datetime")) =!= 14 ||
        col("datetime").isNull
    ).count()

    val invalid = df.filter(
      col("value_cents") === "000000000000" ||
        length(col("datetime")) =!= 14 ||
        col("datetime").isNull ||
        col("transaction_id").isNull ||
        col("merchant_id").isNull
    ).count()

    ValidationResult(
      totalRecords    = total,
      validRecords    = total - invalid,
      invalidRecords  = invalid,
      zeroValueRecords  = zeroValue,
      invalidDateRecords = invalidDate
    )
  }

  def filterValid(df: DataFrame): DataFrame = {
    df.filter(
      col("value_cents") =!= "000000000000" &&
        length(col("datetime")) === 14 &&
        col("datetime").isNotNull &&
        col("transaction_id").isNotNull &&
        col("merchant_id").isNotNull
    )
  }
}