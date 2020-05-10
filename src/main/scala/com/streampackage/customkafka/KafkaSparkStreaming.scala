package com.streampackage.customkafka

import com.streampackage.customkafka.CustomFunctions._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{BasicConfigurator, Level, Logger}
import org.apache.spark.sql.streaming.Trigger

object KafkaSparkStreaming {

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load

    // Create spark session.
    val spark = SparkSession
      .builder
      .appName("KafkaSparkStreamingApp")
      .getOrCreate()

    setupLogging()

    import spark.implicits._
    val lines = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers",conf.getString("bootstrap.server"))
      .option("subscribe",conf.getString("topic"))
      .load().select('value cast "string")

    val words = lines.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    query.awaitTermination()
  }

}
