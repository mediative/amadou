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

package com.mediative.amadou.bigquery

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.googleapis.json.GoogleJsonError
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.model._
import com.google.api.services.bigquery.{ Bigquery, BigqueryScopes }
import com.google.cloud.hadoop.io.bigquery._
import com.google.common.cache.{ CacheBuilder, CacheLoader, LoadingCache }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.Progressable
//import org.joda.time.Instant
//import org.joda.time.format.DateTimeFormat
import org.slf4j.{ Logger, LoggerFactory }

import scala.collection.JavaConverters._
import scala.util.{ Random, Try }
import scala.util.control.NonFatal

private[bigquery] object BigQueryClient {
  val STAGING_DATASET_PREFIX = "bq.staging_dataset.prefix"
  val STAGING_DATASET_PREFIX_DEFAULT = "spark_bigquery_staging_"
  val STAGING_DATASET_LOCATION = "bq.staging_dataset.location"
  val STAGING_DATASET_LOCATION_DEFAULT = "US"
  val STAGING_DATASET_TABLE_EXPIRATION_MS = 86400000L
  val STAGING_DATASET_DESCRIPTION = "Spark BigQuery staging dataset"

  private var instance: BigQueryClient = null

  def getInstance(conf: Configuration): BigQueryClient = {
    if (instance == null) {
      instance = new BigQueryClient(conf)
    }
    instance
  }
}

private[bigquery] class BigQueryClient(conf: Configuration) {

  import BigQueryClient._

  private val logger: Logger = LoggerFactory.getLogger(classOf[BigQueryClient])

  private val SCOPES = List(BigqueryScopes.BIGQUERY).asJava

  private val bigquery: Bigquery = {
    val credential = GoogleCredential.getApplicationDefault.createScoped(SCOPES)
    new Bigquery.Builder(new NetHttpTransport, new JacksonFactory, credential)
      .setApplicationName("spark-bigquery")
      .build()
  }

  private def projectId: String = conf.get(BigQueryConfiguration.PROJECT_ID_KEY)

  /*
  private val queryCache: LoadingCache[String, TableReference] =
    CacheBuilder.newBuilder()
      .expireAfterWrite(STAGING_DATASET_TABLE_EXPIRATION_MS, TimeUnit.MILLISECONDS)
      .build[String, TableReference](new CacheLoader[String, TableReference] {
      override def load(key: String): TableReference = {
        val sqlQuery = key
        logger.info(s"Executing query $sqlQuery")

        val location = conf.get(STAGING_DATASET_LOCATION, STAGING_DATASET_LOCATION_DEFAULT)
        val destinationTable = temporaryTable(location)
        val tableName = BigQueryStrings.toString(destinationTable)
        logger.info(s"Destination table: $destinationTable")

        val job = createQueryJob(sqlQuery, destinationTable, dryRun = false)
        waitForJob(job)
        destinationTable
      }
    })
    */

  private def inConsole = Thread.currentThread().getStackTrace.exists(
    _.getClassName.startsWith("scala.tools.nsc.interpreter."))
  private val PRIORITY = if (inConsole) "INTERACTIVE" else "BATCH"
  private val TABLE_ID_PREFIX = "spark_bigquery"
  private val JOB_ID_PREFIX = "spark_bigquery"
  //private val TIME_FORMATTER = DateTimeFormat.forPattern("yyyyMMddHHmmss")

  /** Matcher for "Missing table" errors. */
  object TableNotFound {
    def unapply(error: Throwable): Option[GoogleJsonError.ErrorInfo] = error match {
      case error: GoogleJsonResponseException =>
        Some(error.getDetails)
          .filter(_.getCode == 404)
          .flatMap(_.getErrors.asScala.find(_.getReason == "notFound"))
      case _ => None
    }
  }

  /**
   * Checks if data has been loaded into a table for a specific date.
   *
   * Performs the following checks and returns false if either succeeds:
   *  - the table does not exists
   *  - the table never modified on or after the specified date
   *  - the table does not contain any data for the specified date
   *
   * The last check will query the table and check whether any entries exists for
   * the column provided via `lastModifiedColumn`.
   */
  def hasDataForDate(table: TableReference, date: java.sql.Date, lastModifiedColumn: String): Boolean = {
    val wasModifiedToday = Try {
      val bqTable = bigquery.tables().get(table.getProjectId, table.getDatasetId, table.getTableId).execute()
      bqTable.getLastModifiedTime.longValue() >= date.getTime
    } recover {
      case TableNotFound(message) => false
    }

    def partitionHasData = {
      val partition = s"${table.getProjectId}:${table.getDatasetId}.${table.getTableId}"
      val job = createQueryJob(
        s"SELECT count(*) > 1 FROM [$partition] WHERE $lastModifiedColumn >= TIMESTAMP($date) LIMIT 1",
        null,
        false,
        "INTERACTIVE"
      )
      waitForJob(job)
      val result = bigquery.jobs.getQueryResults(projectId, job.getJobReference.getJobId).execute()
      val value = result.getRows.get(0).getF.get(0).getV
      value == "true"
    }

    wasModifiedToday.get && partitionHasData
  }

  // XXX: Workaround to support CREATE_IF_NEEDED for date-partitioned tables
  private def createPartitionedTableIfMissing(destinationTable: TableReference): Unit = {
    val table = new TableReference()
      .setProjectId(destinationTable.getProjectId)
      .setDatasetId(destinationTable.getDatasetId)
      .setTableId(destinationTable.getTableId.replaceAll("[$].*", ""))

    val result = Try {
      bigquery.tables().get(table.getProjectId, table.getDatasetId, table.getTableId).execute()
    } recover {
      case TableNotFound(message) =>
        val tableConfiguration = new Table()
          .setTableReference(table)
          .setTimePartitioning(new TimePartitioning().setType("DAY"))
        logger.info(s"Creating date-partitioned table using $tableConfiguration")
        bigquery.tables().insert(table.getProjectId, table.getDatasetId, tableConfiguration).execute()
    }

    result.get
    ()
  }

  /**
   * Load an JSON data set on GCS to a BigQuery table.
   */
  def load(gcsPath: String, destinationTable: TableReference, schema: TableSchema,
    writeDisposition: WriteDisposition.Value = null,
    createDisposition: CreateDisposition.Value = null): Unit = {
    val tableName = BigQueryStrings.toString(destinationTable)
    logger.info(s"Loading $gcsPath into $tableName using ${writeDisposition}/$createDisposition")
    var loadConfig = new JobConfigurationLoad()
      .setDestinationTable(destinationTable)
      .setSourceFormat("NEWLINE_DELIMITED_JSON")
      .setSourceUris(List(gcsPath + "/*.json").asJava)
    if (writeDisposition != null) {
      loadConfig = loadConfig.setWriteDisposition(writeDisposition.toString)
    }
    if (createDisposition != null) {
      if (createDisposition == CreateDisposition.CREATE_IF_NEEDED && destinationTable.getTableId.contains("$"))
        createPartitionedTableIfMissing(destinationTable)
      else
        loadConfig = loadConfig.setCreateDisposition(createDisposition.toString)
    }
    loadConfig.setSchema(schema)

    val jobConfig = new JobConfiguration().setLoad(loadConfig)
    val jobReference = createJobReference(projectId, JOB_ID_PREFIX)
    val job = new Job().setConfiguration(jobConfig).setJobReference(jobReference)
    bigquery.jobs().insert(projectId, job).execute()
    waitForJob(job)
  }

  private def waitForJob(job: Job): Unit = {
    BigQueryUtils.waitForJobCompletion(bigquery, projectId, job.getJobReference, new Progressable {
      override def progress(): Unit = println(".")
    })
  }

  private def stagingDataset(location: String): DatasetReference = {
    // Create staging dataset if it does not already exist
    val prefix = conf.get(STAGING_DATASET_PREFIX, STAGING_DATASET_PREFIX_DEFAULT)
    val datasetId = prefix + location.toLowerCase
    try {
      bigquery.datasets().get(projectId, datasetId).execute()
      logger.info(s"Staging dataset $projectId:$datasetId already exists")
    } catch {
      case e: GoogleJsonResponseException if e.getStatusCode == 404 =>
        logger.info(s"Creating staging dataset $projectId:$datasetId")
        val dsRef = new DatasetReference().setProjectId(projectId).setDatasetId(datasetId)
        val ds = new Dataset()
          .setDatasetReference(dsRef)
          .setDefaultTableExpirationMs(STAGING_DATASET_TABLE_EXPIRATION_MS)
          .setDescription(STAGING_DATASET_DESCRIPTION)
          .setLocation(location)
        bigquery
          .datasets()
          .insert(projectId, ds)
          .execute()

      case NonFatal(e) => throw e
    }
    new DatasetReference().setProjectId(projectId).setDatasetId(datasetId)
  }

  /*
  private def temporaryTable(location: String): TableReference = {
    val now = Instant.now().toString(TIME_FORMATTER)
    val tableId = TABLE_ID_PREFIX + "_" + now + "_" + Random.nextInt(Int.MaxValue)
    new TableReference()
      .setProjectId(projectId)
      .setDatasetId(stagingDataset(location).getDatasetId)
      .setTableId(tableId)
  }
  */

  private def createQueryJob(sqlQuery: String,
    destinationTable: TableReference,
    dryRun: Boolean,
    priority: String = PRIORITY): Job = {
    var queryConfig = new JobConfigurationQuery()
      .setQuery(sqlQuery)
      .setPriority(priority)
      .setCreateDisposition("CREATE_IF_NEEDED")
      .setWriteDisposition("WRITE_EMPTY")
    if (destinationTable != null) {
      queryConfig = queryConfig
        .setDestinationTable(destinationTable)
        .setAllowLargeResults(true)
    }

    val jobConfig = new JobConfiguration().setQuery(queryConfig).setDryRun(dryRun)
    val jobReference = createJobReference(projectId, JOB_ID_PREFIX)
    val job = new Job().setConfiguration(jobConfig).setJobReference(jobReference)
    bigquery.jobs().insert(projectId, job).execute()
  }

  private def createJobReference(projectId: String, jobIdPrefix: String): JobReference = {
    val fullJobId = projectId + "-" + UUID.randomUUID().toString
    new JobReference().setProjectId(projectId).setJobId(fullJobId)
  }

}
