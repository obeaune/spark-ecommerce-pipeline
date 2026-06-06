package com.caroline.dataengineering.config

import com.typesafe.config.ConfigFactory

object AppConfig {
  private val config = ConfigFactory.load()

  val basePath: String = config.getString("pipeline.base-path")
}