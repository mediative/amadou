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

import java.sql._
import java.util.Properties

object DatabaseUtils extends Logging {

  /**
   * Run a sequence of database scripts.
   */
  def runScripts(url: String, properties: Properties, scripts: String*): Unit =
    for {
      connection <- ManagedIO(DriverManager.getConnection(url, properties))
      statement  <- ManagedIO(connection.createStatement)
      sql        <- scripts
    } {
      logger.info(s"Executing SQL script in ${url}: $sql")
      statement.executeUpdate(sql)
    }
}
