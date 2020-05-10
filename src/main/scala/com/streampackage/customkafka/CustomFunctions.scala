package com.streampackage.customkafka

import org.apache.log4j.{BasicConfigurator, Level, Logger}

object CustomFunctions {

  /** Reducing the logging level to print just ERROR. */

  def setupLogging() = {
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }
}
