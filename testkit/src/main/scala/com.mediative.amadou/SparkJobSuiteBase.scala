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

import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite
import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Base class which provides an environment for running tasks.
 */
trait SparkJobSuiteBase extends DatasetSuiteBase with BeforeAndAfterAll { self: Suite =>
  val config     = ConfigFactory.load()
  val fileSystem = FileSystem.get(new Configuration())

  System.setProperty("derby.stream.error.file", "target/spark-warehouse-derby.log")

  override def beforeAll() = {
    super.beforeAll()

    if (config.hasPath("hdfs.root")) {
      val rootUrl = config.getString("hdfs.root")
      val path    = new Path(rootUrl)

      /* Cleanup the HDFS root before running the test. */
      if (rootUrl.startsWith("target/") && fileSystem.exists(path)) {
        assert(fileSystem.delete(path, true))
      }
    }
    ()
  }
}
