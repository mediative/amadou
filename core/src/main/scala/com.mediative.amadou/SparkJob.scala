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

import org.apache.spark.sql._

object SparkJob {

  /**
   * Maximum number of dates considered from a schedule.
   */
  val MaxScheduledDates = 60
}

/**
 * A Spark ETL job
 *
 * Jobs have a schedule of dates which is filtered using job specific
 * logic and run in chronological order. For example, for jobs which run
 * each day and at end of month, we check if it ran correctly "today"
 * and if not we traverse if it ran for previous "month's end" etc.,
 * until we find a completed run or we have traversed some magical max
 * number.
 *
 * The schedule can be overridden by defining the `start` environment
 * variable with a date formatted as "yyyy-MM-dd". In this case the job
 * is only run the the specified date.
 */
trait SparkJob extends Logging {
  def shouldRunForDate(spark: SparkSession, date: DateInterval): Boolean
  def stages: Stage[SparkSession, _]
}
