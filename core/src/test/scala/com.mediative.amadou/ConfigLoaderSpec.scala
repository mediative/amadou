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
import com.typesafe.config.ConfigFactory
import java.util.Properties

object ConfigLoaderSpec {
  case class Database(url: String, properties: Properties)
}

class ConfigLoaderSpec extends WordSpec with Matchers with ConfigLoader {
  import ConfigLoaderSpec.Database

  "propertiesValueReader" should {
    "load from given path" in {
      val config =
        ConfigFactory.parseString("""
        database {
          url = "jdbc:postgresql:testdb"
          properties = src/test/resources/config-reader-spec.properties
        }
      """)
      val db = config.as[Database]("database")
      db.properties.size should be(2)
      db.properties.getProperty("user") should be("john")
      db.properties.getProperty("pass") should be("secret")
    }

    "be empty when no path is given" in {
      val config = ConfigFactory.parseString("""
        database.url = "jdbc:postgresql:testdb"
      """)
      val db     = config.as[Database]("database")
      db.properties.isEmpty should be(true)
    }

    "fail when given path does not exist" in {
      val config =
        ConfigFactory.parseString("""
        database {
          url = "jdbc:postgresql:testdb"
          properties = src/test/resources/doesn-not-exists.properties
        }
      """)

      the[java.io.FileNotFoundException] thrownBy {
        config.as[Database]("database")
      } should have message "src/test/resources/doesn-not-exists.properties (No such file or directory)"
    }
  }
}
