package com.streampackage.customkafka

import com.streampackage.customkafka.CustomFunctions._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{BasicConfigurator, Level, Logger}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions.{explode,to_timestamp}
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, LongType, StringType, StructField, StructType}

object KafkaSparkStreaming {

  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.load
    val spark = SparkSession
      .builder
      .appName("KafkaSparkStreamingApp")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schema = StructType(
      List(
        StructField("Countries", ArrayType(StructType(
          Array(
            StructField("Country",StringType),
            StructField("CountryCode",StringType),
            StructField("Date",StringType),
            StructField("NewConfirmed",LongType),
            StructField("NewDeaths",LongType),
            StructField("NewRecovered",LongType),
            StructField("Slug",StringType),
            StructField("TotalConfirmed",LongType),
            StructField("TotalDeaths",LongType),
            StructField("TotalRecovered",LongType))
          )
        )),
        StructField("Date", StringType, true),
        StructField("Global",StringType,true)
       /* StructField("Global",ArrayType(
          StructType(
            List(
              StructField("NewConfirmed",LongType),
              StructField("NewDeaths",LongType),
              StructField("NewRecovered",LongType),
              StructField("TotalConfirmed",LongType),
              StructField("TotalDeaths",LongType),
              StructField("TotalRecovered",LongType)

            )
          )
        ))*/
    ))

    val df = spark.readStream
      .option("multiLine", true)
      .schema(schema)
      .json(conf.getString("filePath"))

    df.printSchema()
    import spark.implicits._

    val df1 = df.withColumn("Countries",explode($"Countries")).select("Countries.*")

    val df2 = df1.withColumn("event_date",$"Date".cast("timestamp")).drop("Date")
    df2.printSchema()
    df2.writeStream
      .format("parquet")
      .outputMode("append")
      .option("path", "/user/training/covid")
      .option("checkpointLocation", "hdfs:///user/training/spark-checkpointing")
      .start()
      .awaitTermination()

    //Enable below code to check the ouput on console.
    /*df2.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()*/
  }

}
