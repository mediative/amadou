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
package monitoring

import com.typesafe.config.Config
import scala.language.implicitConversions

case class ProcessContext(
    jobId: String,
    processingDate: DateInterval,
    processId: String = java.util.UUID.randomUUID().toString,
    startTime: Long = System.currentTimeMillis) {
  def duration: Long = System.currentTimeMillis - startTime
}

object MessagingSystem {
  def create(config: Config) = config.hasPath("kafka.bootstrap.servers") match {
    case true  => new KafkaMessagingSystem(config.getConfig("kafka"))
    case false => new PrintMessagingSystem
  }
}

abstract class MessagingSystem {
  def publish(topic: String, message: String): Unit
  def stop(): Unit = ()

  // high level helper functions
  def publishProcessStarting(context: ProcessContext): Unit =
    publishRunEvent(context, state = Processing)

  def publishProcessComplete(context: ProcessContext): Unit =
    publishRunEvent(context, state = Complete)

  def publishProcessFailed(context: ProcessContext, failure: Throwable): Unit =
    publishRunEvent(context, state = Failed, message = failureToMessage(failure))

  def publishStageStarting(context: ProcessContext, stage: String, message: String = ""): Unit =
    publishStageEvent(context, state = Processing, stage = stage, message = message)

  def publishStageComplete(context: ProcessContext, stage: String, message: String = ""): Unit =
    publishStageEvent(context, state = Complete, stage = stage, message = message)

  def publishStageRetrying(context: ProcessContext, stage: String): Unit =
    publishStageEvent(context, state = Retrying, stage = stage)

  def publishStageFailed(context: ProcessContext, stage: String, failure: Throwable): Unit =
    publishStageEvent(context, state = Failed, stage = stage, message = failureToMessage(failure))

  def publishMetrics(context: ProcessContext, stage: String, metrics: Map[String, Double]): Unit =
    publish(
      "metrics",
      MetricsEvent(
        jobId = context.jobId,
        processId = context.processId,
        timestamp = System.currentTimeMillis(),
        stage = stage,
        message = metrics
      ))

  private def publishRunEvent(context: ProcessContext, state: StateRecord, message: String = "") =
    publish(
      "jobs",
      RunEvent(
        jobId = context.jobId,
        processId = context.processId,
        timestamp = System.currentTimeMillis(),
        processingDate = context.processingDate.format("yyyy-MM-dd"),
        state = state.identifier,
        duration = state match {
          case Complete => context.duration
          case _        => 0L
        },
        message = message
      ))

  private def publishStageEvent(
      context: ProcessContext,
      state: StateRecord,
      stage: String,
      message: String = ""): Unit =
    publish(
      "stages",
      StageEvent(
        jobId = context.jobId,
        processId = context.processId,
        timestamp = System.currentTimeMillis(),
        stage = stage,
        state = state.identifier,
        duration = state match {
          case Complete => context.duration
          case _        => 0L
        },
        message = message
      ))

  private implicit def processMessageToString(message: RunEvent): String =
    upickle.default.write(message)
  private implicit def stageMessageToString(message: StageEvent): String =
    upickle.default.write(message)
  private implicit def metricsMessageToString(message: MetricsEvent): String =
    upickle.default.write(message)
  private def failureToMessage(failure: Throwable) =
    s"${failure.getClass.getName}: ${failure.getMessage}"
}
