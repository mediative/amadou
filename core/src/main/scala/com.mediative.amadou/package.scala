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

package com.mediative

import org.apache.spark.sql._

package object amadou {
  type Config  = com.typesafe.config.Config
  type Gauge   = io.prometheus.client.Gauge
  type Counter = io.prometheus.client.Counter

  implicit class SparkHdfsUrlReaderOps(val self: DataFrameReader) extends AnyVal {
    def csv(url: HdfsUrl*)      = self.csv(url.map(_.toString): _*)
    def json(url: HdfsUrl*)     = self.json(url.map(_.toString): _*)
    def load(url: HdfsUrl*)     = self.load(url.map(_.toString): _*)
    def orc(url: HdfsUrl*)      = self.orc(url.map(_.toString): _*)
    def parquet(url: HdfsUrl*)  = self.parquet(url.map(_.toString): _*)
    def text(url: HdfsUrl*)     = self.text(url.map(_.toString): _*)
    def textFile(url: HdfsUrl*) = self.textFile(url.map(_.toString): _*)
  }

  implicit class SparkHdfsUrlWriteOps[T](val self: DataFrameWriter[T]) extends AnyVal {
    def csv(url: HdfsUrl)     = self.csv(url.toString)
    def json(url: HdfsUrl)    = self.json(url.toString)
    def save(url: HdfsUrl)    = self.save(url.toString)
    def orc(url: HdfsUrl)     = self.orc(url.toString)
    def parquet(url: HdfsUrl) = self.parquet(url.toString)
    def text(url: HdfsUrl)    = self.text(url.toString)
  }

  implicit class SymbolToStage(val self: Symbol) extends AnyVal {
    def stage[I, T](f: Stage.Context[I] => T)                      = Stage(self.name)(f)
    def source[T](read: Stage.Context[SparkSession] => Dataset[T]) = Stage.source(self.name)(read)
    def transform[S, T](transform: Stage.Context[Dataset[S]] => Dataset[T]) =
      Stage.transform(self.name)(transform)
    def sink[T](write: Stage.Context[Dataset[T]] => Unit) = Stage.sink(self.name)(write)
  }

  /**
   * Helpers for working with Dataset columns.
   */
  implicit class SparkColumnOps(val self: Column) extends AnyVal {
    import org.apache.spark.sql.functions.when

    /**
     * Filter out values which are null, empty or "null".
     *
     * @example
     * {{{
     * dataset.filter($"UserId".isDefined)
     * }}}
     */
    def isDefined: Column =
      self.isNotNull && !(self === "") && !(self === "null")

    /**
     * Replace "null" string with NULL value.
     *
     * @example
     * {{{
     * dataset.select($"Description".nullify as "description")
     * }}}
     */
    def nullify: Column =
      when(self === "null", null).otherwise(self)
  }
}
