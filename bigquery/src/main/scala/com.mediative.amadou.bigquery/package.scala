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

import com.google.api.services.bigquery.model._
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
import com.google.cloud.hadoop.io.bigquery._
import org.apache.hadoop.fs.{FileSystem, Path}
import net.ceedubs.ficus.readers.ValueReader
import net.ceedubs.ficus.FicusInstances

import org.apache.spark.sql.{Dataset, SparkSession, Encoder}
import java.util.concurrent.ThreadLocalRandom
import scala.collection.JavaConversions._

package object bigquery extends FicusInstances {

  object CreateDisposition extends Enumeration {
    val CREATE_IF_NEEDED, CREATE_NEVER = Value
  }

  object WriteDisposition extends Enumeration {
    val WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY = Value
  }

  val BQ_CSV_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss zzz"

  object TableNotFound {
    import com.google.api.client.googleapis.json.GoogleJsonResponseException
    import com.google.api.client.googleapis.json.GoogleJsonError
    import scala.collection.JavaConverters._

    def unapply(error: Throwable): Option[GoogleJsonError.ErrorInfo] = error match {
      case error: GoogleJsonResponseException =>
        Some(error.getDetails)
          .filter(_.getCode == 404)
          .flatMap(_.getErrors.asScala.find(_.getReason == "notFound"))
      case _ => None
    }
  }

  def tableHasDataForDate(
      spark: SparkSession,
      table: TableReference,
      date: java.sql.Date,
      column: String): Boolean = {
    val bq = BigQueryClient.getInstance(spark.sparkContext.hadoopConfiguration)
    bq.hasDataForDate(table, date, column)
  }

  /**
   * Enhanced version of SparkSession with BigQuery support.
   */
  implicit class BigQuerySparkSession(self: SparkSession) {

    val sc      = self.sqlContext.sparkContext
    val conf    = sc.hadoopConfiguration
    lazy val bq = BigQueryClient.getInstance(conf)

    // Register GCS implementation
    if (conf.get("fs.gs.impl") == null) {
      conf.set("fs.gs.impl", classOf[GoogleHadoopFileSystem].getName)
    }

    /**
     * Set GCP project ID for BigQuery.
     */
    def setBigQueryProjectId(projectId: String): Unit = {
      conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)

      // Also set project ID for GCS connector
      if (conf.get("fs.gs.project.id") == null) {
        conf.set("fs.gs.project.id", projectId)
      }
    }

    /**
     * Set GCS bucket for temporary BigQuery files.
     */
    def setBigQueryGcsBucket(gcsBucket: String): Unit =
      conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, gcsBucket)

    /**
     * Set BigQuery dataset location, e.g. US, EU.
     */
    def setBigQueryDatasetLocation(location: String): Unit =
      conf.set(BigQueryClient.STAGING_DATASET_LOCATION, location)

    /**
     * Set GCP JSON key file.
     */
    def setGcpJsonKeyFile(jsonKeyFile: String): Unit = {
      conf.set("mapred.bq.auth.service.account.json.keyfile", jsonKeyFile)
      conf.set("fs.gs.auth.service.account.json.keyfile", jsonKeyFile)
    }

    /**
     * Set GCP pk12 key file.
     */
    def setGcpPk12KeyFile(pk12KeyFile: String): Unit = {
      conf.set("google.cloud.auth.service.account.keyfile", pk12KeyFile)
      conf.set("mapred.bq.auth.service.account.keyfile", pk12KeyFile)
      conf.set("fs.gs.auth.service.account.keyfile", pk12KeyFile)
    }

    /**
     * Reads a CSV extract of a BigQuery table.
     */
    def readBigQueryCSVExtract[T: Encoder](url: String, dateFormat: String): Seq[T] =
      self.read
        .option("header", true)
        .option("timestampFormat", dateFormat)
        .option("escape", "\"")
        .schema(implicitly[Encoder[T]].schema)
        .csv(url)
        .as[T]
        .collect
        .toSeq

    def readBigQueryCSVExtract[T: Encoder](
        url: HdfsUrl,
        dateFormat: String = BQ_CSV_DATE_FORMAT): Seq[T] =
      readBigQueryCSVExtract(url.toString, dateFormat)
  }

  /**
   * Enhanced version of DataFrame with BigQuery support.
   */
  implicit class BigQueryDataset[T](self: Dataset[T]) {

    val sqlContext = self.sqlContext
    val conf       = sqlContext.sparkContext.hadoopConfiguration
    val bq         = BigQueryClient.getInstance(conf)

    /**
     * Save a DataFrame to a BigQuery table.
     */
    def saveAsBigQueryTable(
        tableRef: TableReference,
        writeDisposition: WriteDisposition.Value,
        createDisposition: CreateDisposition.Value): Unit = {
      val bucket = conf.get(BigQueryConfiguration.GCS_BUCKET_KEY)
      val temp =
        s"spark-bigquery-${System.currentTimeMillis()}=${ThreadLocalRandom.current.nextInt(Int.MaxValue)}"
      val gcsPath = s"gs://$bucket/spark-bigquery-tmp/$temp"
      self.write.json(gcsPath)

      val schemaFields = self.schema.fields.map { field =>
        import org.apache.spark.sql.types._

        val fieldType = field.dataType match {
          case BooleanType    => "BOOLEAN"
          case LongType       => "INTEGER"
          case IntegerType    => "INTEGER"
          case StringType     => "STRING"
          case DoubleType     => "FLOAT"
          case TimestampType  => "TIMESTAMP"
          case _: DecimalType => "INTEGER"
        }
        new TableFieldSchema().setName(field.name).setType(fieldType)
      }.toList

      val tableSchema = new TableSchema().setFields(schemaFields)

      bq.load(gcsPath, tableRef, tableSchema, writeDisposition, createDisposition)
      delete(new Path(gcsPath))
    }

    private def delete(path: Path): Unit = {
      val fs = FileSystem.get(path.toUri, conf)
      fs.delete(path, true)
      ()
    }

  }

  implicit val valueReader: ValueReader[BigQueryTable.PartitionStrategy] =
    ValueReader[String].map {
      _ match {
        case "month" => BigQueryTable.PartitionByMonth
        case "day"   => BigQueryTable.PartitionByDay
        case other   => sys.error(s"Unknown partition strategy")
      }
    }
}
