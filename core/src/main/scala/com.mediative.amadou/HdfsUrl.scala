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

import org.apache.hadoop.fs.{Path, FSDataOutputStream}
import org.apache.spark.sql.SparkSession

/**
 * Utility class for working with HDFS URLs.
 *
 * It allows to customize how dates are formatted and then
 * {{{
 * scala> HdfsUrl("some/path") / "somewhere" / Day(2016, 8, 28)
 * res1: HdfsUrl = some/path/somewhere/2016-08-28
 * scala> HdfsUrl("root", dateFormat = Some("'year='yyyy/'month'=MM")) / Day(2016, 8, 28)
 * res2: HdfsUrl = root/year=2016/month=08
 * }}}
 */
case class HdfsUrl(url: String, dateFormat: Option[String] = None) {
  def path = new Path(url)

  def /(subPath: String): HdfsUrl =
    copy(url = new Path(path, subPath).toString)

  def /(date: DateInterval): HdfsUrl = {
    val datePath = dateFormat.fold(date.toString)(date.format)
    this./(datePath)
  }

  def exists(spark: SparkSession) = fileSystem(spark).exists(path)

  def open[T](spark: SparkSession)(f: FSDataOutputStream => T): T = {
    val stream = fileSystem(spark).create(path)
    try {
      f(stream)
    } finally {
      stream.close()
    }
  }

  def fileSystem(spark: SparkSession) =
    path.getFileSystem(spark.sparkContext.hadoopConfiguration)

  override def toString = path.toString
}
