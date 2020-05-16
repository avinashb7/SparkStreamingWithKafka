package com.streampackage.customkafka

import jdk.internal.net.http.frame.DataFrame
import org.apache.log4j.{BasicConfigurator, Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object CustomFunctions {

  /** Reducing the logging level to print just ERROR. */

  def setupLogging() = {
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }
}
