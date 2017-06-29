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

object MessagingSystem {
  def create(config: Config) = config.hasPath("kafka.bootstrap.servers") match {
    case true  => new KafkaMessagingSystem(config.getConfig("kafka"))
    case false => new PrintMessagingSystem
  }

  trait Context {
    def jobId: String
    def eventDate: DateInterval
    def processId: String = java.util.UUID.randomUUID().toString
    def startTime: Long   = System.currentTimeMillis
    def duration: Long    = System.currentTimeMillis - startTime
  }
}

abstract class MessagingSystem {
  import MessagingSystem.Context

  def publish(topic: String, message: String): Unit
  def stop(): Unit = ()

  // high level helper functions
  def publishProcessStarting(context: Context): Unit =
    publishRunEvent(context, state = Processing)

  def publishProcessComplete(context: Context): Unit =
    publishRunEvent(context, state = Complete)

  def publishProcessFailed(context: Context, failure: Throwable): Unit =
    publishRunEvent(context, state = Failed, message = failureToMessage(failure))

  def publishStageStarting(context: Context, stage: String, message: String = ""): Unit =
    publishStageEvent(context, state = Processing, stage = stage, message = message)

  def publishStageComplete(context: Context, stage: String, message: String = ""): Unit =
    publishStageEvent(context, state = Complete, stage = stage, message = message)

  def publishStageRetrying(context: Context, stage: String): Unit =
    publishStageEvent(context, state = Retrying, stage = stage)

  def publishStageFailed(context: Context, stage: String, failure: Throwable): Unit =
    publishStageEvent(context, state = Failed, stage = stage, message = failureToMessage(failure))

  def publishMetrics(context: Context, stage: String, metrics: Map[String, Double]): Unit =
    publish(
      "metrics",
      MetricsEvent(
        jobId = context.jobId,
        processId = context.processId,
        timestamp = System.currentTimeMillis(),
        stage = stage,
        message = metrics
      ))

  private def publishRunEvent(context: Context, state: StateRecord, message: String = "") =
    publish(
      "jobs",
      RunEvent(
        jobId = context.jobId,
        processId = context.processId,
        timestamp = System.currentTimeMillis(),
        processingDate = context.eventDate.format("yyyy-MM-dd"),
        state = state.identifier,
        duration = state match {
          case Complete => context.duration
          case _        => 0L
        },
        message = message
      ))

  private def publishStageEvent(
      context: Context,
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
