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

/**
 * Process and stage states registry.
 */
sealed trait StateRecord {
  def identifier = toString
}

case object Processing extends StateRecord
case object Retrying   extends StateRecord
case object Failed     extends StateRecord
case object Complete   extends StateRecord

case class MetricsEvent(
    jobId: String,
    processId: String,
    stage: String,
    timestamp: Long,
    message: Map[String, Double])

case class RunEvent(
    jobId: String,
    processId: String,
    state: String,
    processingDate: String,
    timestamp: Long,
    duration: Long,
    message: String)

case class StageEvent(
    jobId: String,
    processId: String,
    stage: String,
    state: String,
    timestamp: Long,
    duration: Long,
    message: String)
