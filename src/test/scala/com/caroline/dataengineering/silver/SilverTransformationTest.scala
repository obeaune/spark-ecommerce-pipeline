package com.caroline.dataengineering.silver

import com.caroline.dataengineering.config.SparkConfig
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class SilverTransformationTest extends AnyFunSuite with BeforeAndAfterAll {

  val spark = SparkConfig.createSparkSession("SilverTransformationTest")

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("value_cents should be converted to decimal amount") {
    val schema = StructType(Seq(
      StructField("record_type", StringType),
      StructField("transaction_id", StringType),
      StructField("merchant_id", StringType),
      StructField("value_cents", StringType),
      StructField("datetime", StringType),
      StructField("payment_type", StringType),
      StructField("installments", StringType),
      StructField("status", StringType)
    ))

    val data = Seq(
      Row("01", "1234567890123456", "1234567890", "000000004990", "20240315143022", "02", "01", "00")
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val result = df
      .withColumn("amount", (col("value_cents").cast("long") / 100).cast("decimal(15,2)"))

    val amount = result.select("amount").as[java.math.BigDecimal].first()
    assert(amount.compareTo(new java.math.BigDecimal("49.90")) == 0)
  }

  test("payment_type 02 should map to credit") {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df = Seq(("02")).toDF("payment_type")

    val result = df.withColumn("payment_description",
      when(col("payment_type") === "01", "debit")
        .when(col("payment_type") === "02", "credit")
        .when(col("payment_type") === "03", "pix")
        .otherwise("unknown"))

    val description = result.select("payment_description").as[String].first()
    assert(description == "credit")
  }

  test("status 00 should map to approved") {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df = Seq(("00")).toDF("status")

    val result = df.withColumn("status_description",
      when(col("status") === "00", "approved")
        .when(col("status") === "01", "denied")
        .when(col("status") === "02", "cancelled")
        .otherwise("unknown"))

    val description = result.select("status_description").as[String].first()
    assert(description == "approved")
  }
}