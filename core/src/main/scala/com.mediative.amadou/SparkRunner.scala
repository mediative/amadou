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

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}
import com.typesafe.config._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import com.amazonaws.auth.profile.ProfilesConfigFile
import io.prometheus.client._

import monitoring.{MessagingSystem, ProcessContext}

abstract class SparkRunner[Job <: SparkJob] extends Logging with ScheduleDsl with ConfigLoader {
  def jobName: String
  def schedule: Schedule
  def createJob(config: Config): Job
  def recordsProcessed: Collector = sparkRecordsRead

  def main(args: Array[String]): Unit =
    Try(run) match {
      case Failure(failure) =>
        logger.error("Spark job failed", failure)
        sys.exit(1)
      case _ =>
        sys.exit(0)
    }

  def run(): Unit = {
    val config =
      sys.env
        .get("DEPLOY_ENVIRONMENT")
        .fold(ConfigFactory.empty) { env =>
          ConfigFactory.defaultOverrides().withFallback(ConfigFactory.load(env))
        }
        .withFallback(ConfigFactory.load())

    val messaging = MessagingSystem.create(config)

    val singleDate  = sys.env.get("start").flatMap(Day.parse)
    val job         = createJob(config)
    val sparkConfig = new SparkConf().setAppName(jobName)

    for {
      setting <- config.entrySet.toSeq
      if setting.getKey.startsWith("spark.")
      // `.getString` does not support LIST types
      if setting.getValue.valueType != ConfigValueType.LIST
      // Ensure the key path is unquoted
      key = ConfigUtil.splitPath(setting.getKey).mkString(".")
    } yield sparkConfig.setIfMissing(key, config.getString(setting.getKey))

    val spark = SparkSession.builder
      .config(sparkConfig)
      .getOrCreate()

    /*
     * XXX: Add S3 credentials after creating the Spark session so they are
     * not logged.
     */
    sys.env.get("AWS_CREDENTIALS").foreach { file =>
      val creds = new ProfilesConfigFile(file).getCredentials("default")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", creds.getAWSAccessKeyId)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", creds.getAWSSecretKey)
    }

    spark.sparkContext.addSparkListener(sparkListener)

    val shouldRunForDate: DateInterval => Boolean = singleDate match {
      case Some(date) => date.<=
      case None       => job.shouldRunForDate(spark, _)
    }

    val dates = schedule
      .take(SparkJob.MaxScheduledDates)
      .takeWhile(shouldRunForDate)
      .toList
      .reverse

    logger.info(s"Scheduled dates are: $dates")
    dates.foreach(runForDate(spark, job, messaging))

    messaging.stop()
    spark.stop()
  }

  def runForDate(spark: SparkSession, job: Job, messaging: MessagingSystem)(
      date: DateInterval): Unit = {
    val processContext = ProcessContext(jobName, date)
    val ctx            = new Context(job, processContext, spark, messaging, date, spark)

    messaging.publishProcessStarting(processContext)
    job.stages.run(ctx)
    messaging.publishProcessComplete(processContext)
  }

  class Context[+I](
      job: Job,
      processContext: ProcessContext,
      spark: SparkSession,
      messaging: MessagingSystem,
      date: DateInterval,
      value: I)
      extends Stage.Context(spark, date, value) {

    override def withValue[U](value: U) =
      new Context(job, processContext, spark, messaging, date, value)

    override def run[T](stage: Stage[I, T], result: => T): Stage.Result[T] = {
      def runStage(callCount: Int): Stage.Result[T] = {
        logger.info(s"[$processContext] Running stage ${stage.name} try #$callCount")
        counters.foreach(_.clear())
        messaging.publishStageStarting(processContext, stage.name)
        Try(result) match {
          case v @ Success(_) =>
            messaging.publishStageComplete(processContext, stage.name)
            messaging.publishMetrics(processContext, stage.name, collectMetrics())
            v
          case Failure(failure) =>
            if (callCount >= job.maxRetries) {
              logger.error(s"[$processContext] Giving up after ${job.maxRetries} retries", failure)
              messaging.publishStageFailed(processContext, stage.name, failure)
              messaging.publishProcessFailed(processContext, failure)
              throw failure
            } else {
              logger.error(
                s"[$processContext] Will retry stage ${stage.name} in ${job.delayBetweenRetries}",
                failure)
              messaging.publishStageRetrying(processContext, stage.name)
              Thread.sleep(job.delayBetweenRetries.toMillis)
              runStage(callCount + 1)
            }
        }
      }

      runStage(1)
    }
  }

  /*
   * Metrics management
   */

  private val counters = scala.collection.mutable.ArrayBuffer[Counter]()

  /**
   * Counters will be reset before each job run.
   */
  protected def counter(name: String, help: String, labels: String*): Counter = {
    val collector = Counter.build().name(name).labelNames(labels: _*).help(help).register()
    counters += collector
    collector
  }

  protected def gauge(name: String, help: String, labels: String*): Gauge =
    Gauge.build().name(name).labelNames(labels: _*).help(help).register()

  private def collectMetrics(): Map[String, Double] = {
    def labeledSamples(sample: Collector.MetricFamilySamples.Sample) =
      for {
        (label, labelValue) <- sample.labelNames.toSeq.zip(sample.labelValues.toSeq)
        value               <- Try(labelValue.toDouble).toOption
      } yield s"${sample.name}_$label" -> value

    val metrics = for {
      family <- CollectorRegistry.defaultRegistry.metricFamilySamples()
      sample <- family.samples
      metric <- labeledSamples(sample) :+ (sample.name -> sample.value)
    } yield metric

    val recordsProcessedMetrics = for {
      family <- recordsProcessed.collect
      sample <- family.samples
    } yield ("recordsProcessed" -> sample.value)

    metrics.toMap ++ recordsProcessedMetrics.headOption
  }

  hotspot.DefaultExports.initialize()

  private lazy val sparkBytesRead   = counter("spark_bytes_read", "Number of bytes read.")
  private lazy val sparkRecordsRead = counter("spark_records_read", "Number of records read.")
  private lazy val sparkRecordsWritten =
    counter("spark_records_written", "Number of records written.")

  private def sparkListener() =
    new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        sparkBytesRead.inc(taskEnd.taskMetrics.inputMetrics.bytesRead)
        sparkRecordsRead.inc(taskEnd.taskMetrics.inputMetrics.recordsRead)
        // FIXME: This seems to always be 0L. Look into using information from
        // `taskEnd.taskInfo.accumulables` and/or `taskEnd.taskMetrics.shuffleWriteMetrics.recordsWritten`
        sparkRecordsWritten.inc(taskEnd.taskMetrics.outputMetrics.recordsWritten)
      }
    }
}
