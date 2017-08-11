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

import org.scalatest.{WordSpec, Matchers}

object SparkImplicitsSpec {
  case class Account(id: Int, name: String, externalId: String)
}

class SparkImplicitsSpec extends WordSpec with Matchers with SparkJobSuiteBase {
  import SparkImplicitsSpec._

  "SparkColumnOps.isDefined" should {
    "filter values which are null, empty or the string 'null'" in {
      import spark.implicits._

      val accounts = List(
        Account(0, "Account #0", ""),
        Account(1, "Account #1", "00000000001"),
        Account(2, "Account #2", "null"),
        Account(3, "Account #3", "Not null"),
        Account(4, "Account #4", null),
        Account(5, "Account #5", "00000000002")
      )
      val data = spark.createDataset(accounts)
      data.count shouldBe 6

      val filtered = data.filter($"externalId".isDefined)
      filtered.collect.toList shouldBe List(
        Account(1, "Account #1", "00000000001"),
        Account(3, "Account #3", "Not null"),
        Account(5, "Account #5", "00000000002")
      )
    }
  }

  "SparkColumnOps.nullify" should {
    "turn 'null' string values into null" in {
      import spark.implicits._

      val accounts = List(
        Account(0, "Account #0", ""),
        Account(1, "Account #1", "00000000001"),
        Account(2, "Account #2", "null"),
        Account(3, "Account #3", "Not null"),
        Account(4, "Account #4", null),
        Account(5, "Account #5", "00000000002")
      )
      val data = spark.createDataset(accounts)
      data.count shouldBe 6

      val filtered = data
        .select(
          $"id",
          $"name".nullify as "name",
          $"externalId".nullify as "externalId"
        )
        .as[Account]

      filtered.collect.toList shouldBe List(
        Account(0, "Account #0", ""),
        Account(1, "Account #1", "00000000001"),
        Account(2, "Account #2", null),
        Account(3, "Account #3", "Not null"),
        Account(4, "Account #4", null),
        Account(5, "Account #5", "00000000002")
      )
    }
  }
}
