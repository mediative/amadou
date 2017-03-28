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
import scala.util.Try

sealed trait Stage[-I, +T] { self =>
  def name: String

  def map[U](f: T => U): Stage[I, U] = new Stage[I, U] {
    override def name = self.name
    override def run(ctx: Stage.Context[I]): Stage.Result[U] = self.run(ctx).map(f)
  }

  def flatMap[U](f: T => Stage[T, U]): Stage[I, U] = new Stage[I, U] {
    override def name = self.name
    override def run(ctx: Stage.Context[I]): Stage.Result[U] =
      self.run(ctx).flatMap(data => f(data).run(ctx.withValue(data)))
  }

  def andThen[U](s: Stage[T, U]): Stage[I, U] = flatMap(_ => s)
  def ~>[U](s: Stage[T, U]): Stage[I, U] = andThen(s)

  def run(ctx: Stage.Context[I]): Stage.Result[T]
}

object Stage {
  type Result[A] = Try[A]

  abstract class Context[+I](
      val spark: SparkSession,
      val date: DateInterval,
      val value: I) {
    def withValue[U](value: U): Context[U]
    def run[T](stage: Stage[I, T], result: => T): Result[T] = Try(result)
  }

  object Context {
    def apply(spark: SparkSession, date: DateInterval): Context[SparkSession] =
      new SimpleContext(spark, date, spark)
  }

  class SimpleContext[+I](spark: SparkSession, date: DateInterval, value: I) extends Context[I](spark, date, value) {
    override def withValue[U](value: U) = new SimpleContext(spark, date, value)
  }

  def apply[S, T](stageName: String)(f: Stage.Context[S] => T): Stage[S, T] = new Stage[S, T] {
    override def name = stageName
    override def run(ctx: Stage.Context[S]) = ctx.run(this, f(ctx))
  }

  def source[T](name: String)(read: Stage.Context[SparkSession] => Dataset[T]) =
    Stage(name)(read)
  def transform[S, T](name: String)(transform: Stage.Context[Dataset[S]] => Dataset[T]) =
    Stage(name)(transform)
  def sink[T](name: String)(write: Stage.Context[Dataset[T]] => Unit) =
    Stage(name)((ctx: Stage.Context[Dataset[T]]) => { write(ctx); ctx.value })

  def identity[T] = new Stage[T, T] { self =>
    override def name = "identity"

    override def flatMap[U](f: T => Stage[T, U]): Stage[T, U] = new Stage[T, U] {
      override def name = self.name
      override def run(ctx: Stage.Context[T]): Stage.Result[U] =
        Try(f(ctx.value)).flatMap(stage => stage.run(ctx))
    }

    override def run(ctx: Stage.Context[T]) = ctx.run(this, ctx.value)
  }
}
