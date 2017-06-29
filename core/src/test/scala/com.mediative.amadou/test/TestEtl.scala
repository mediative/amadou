/*
 * Copyright 2017 Mediative
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mediative.amadou
package test

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TestEtl extends SparkRunner[TestEtlJob] {
  val jobName  = "test_etl"
  val schedule = today

  override val recordsProcessed = gauge("test_etl_processed", "Number of processed rows")

  val RawSchema = StructType(
    Array(
      StructField("Object Name", StringType),
      StructField("Object Colour", StringType),
      StructField("Observed Time", TimestampType),
      StructField("Observed Latitude", DoubleType),
      StructField("Observed Longitude", DoubleType),
      StructField("Speed", LongType)
    ))

  case class Clean(
      name: String,
      isPink: Boolean,
      eventDate: java.sql.Timestamp,
      latitude: Double,
      longitude: Double,
      speed: Long,
      processingDate: java.sql.Timestamp)

  override def createJob(config: Config) =
    TestEtlJob(
      config.as[HdfsUrl]("test_etl"),
      config.as[HdfsUrl]("hdfs.raw") / jobName,
      config.as[HdfsUrl]("hdfs.clean") / jobName,
      recordsProcessed
    )

}

case class TestEtlJob(
    testEtlUrl: HdfsUrl,
    rawUrl: HdfsUrl,
    cleanUrl: HdfsUrl,
    recordsProcessed: Gauge)
    extends SparkJob {

  import TestEtl.{RawSchema, Clean}

  def shouldRunForDate(spark: SparkSession, date: DateInterval): Boolean = true

  override val stages =
    clean ~> 'SaveClean.sink[Clean](ctx =>
      ctx.value.write.mode(SaveMode.Overwrite).parquet(cleanUrl / ctx.date))

  def clean: Stage[SparkSession, Dataset[Clean]] =
    for {
      // Read raw input from input
      rawData <- 'ReadRaw.source[Row] { ctx =>
        ctx.spark.read
          .option("header", true)
          .option("dateFormat", "yyyy-MM-dd")
          .schema(RawSchema)
          .csv(testEtlUrl / ctx.date / "*.csv")
      }

      saveRaw <- 'SaveRaw.sink[Row](ctx =>
        ctx.value.write.mode(SaveMode.Overwrite).csv(rawUrl / ctx.date))

      cleanData <- 'CleanData.transform[Row, Clean] { ctx =>
        import ctx.spark.implicits._

        val CleanSchema = implicitly[Encoder[Clean]].schema
        val isPink      = udf((colorName: String) => colorName.compareToIgnoreCase("pink") == 0)

        ctx.value
          .withColumnRenamed("Object Name", "name")
          .withColumnRenamed("Observed Time", "eventDate")
          .withColumnRenamed("Observed Latitude", "latitude")
          .withColumnRenamed("Observed Longitude", "longitude")
          .withColumnRenamed("Speed", "speed")
          .withColumn("isPink", isPink($"Object Colour"))
          .withColumn("processingDate", lit(ctx.date.asTimestamp))
          .select(CleanSchema.fieldNames.map(col): _*)
          .as[Clean]
      }
    } yield cleanData
}
