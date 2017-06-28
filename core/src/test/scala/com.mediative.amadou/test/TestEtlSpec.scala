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

import org.scalatest._
import org.apache.spark.sql.types._

class TestEtlSpec extends FreeSpec with SparkJobSuiteBase {

  val job  = TestEtl.createJob(config)
  val date = Day(2017, 2, 21)

  val cleanSchema = StructType(
    Array(
      StructField("name", StringType),
      StructField("isPink", BooleanType),
      StructField("eventDate", TimestampType),
      StructField("latitude", DoubleType),
      StructField("longitude", DoubleType),
      StructField("speed", LongType),
      StructField("processingDate", TimestampType, false)
    ))

  "TestEtl" - {
    "should have expected schema and number of entries" in {
      val clean = job.clean.run(Stage.Context(spark, date))

      assert(clean.isSuccess)
      assert(clean.get.schema == cleanSchema)
      assert(clean.get.count == 3)
    }

    "should persist raw data" in {
      val rawPath = job.rawUrl / "2017/02/21"

      assert(rawPath.exists(spark), s"$rawPath does not exist")
    }

    "should persist clean data" in {
      val cleanPath = job.cleanUrl / "date=2017-02-21"

      job.stages.run(Stage.Context(spark, date))
      assert(cleanPath.exists(spark), s"$cleanPath does not exist")
    }
  }
}
