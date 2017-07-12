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
import scala.util.{Failure, Success, Try}

sealed trait Stage[-I, +T] { self =>
  def name: String

  def map[U](f: T => U): Stage[I, U] = new Stage[I, U] {
    override def name                                        = self.name
    override def run(ctx: Stage.Context[I]): Stage.Result[U] = self.run(ctx).map(f)
  }

  def flatMap[U](f: T => Stage[T, U]): Stage[I, U] = new Stage[I, U] {
    override def name = self.name
    override def run(ctx: Stage.Context[I]): Stage.Result[U] =
      self.run(ctx).flatMap(data => f(data).run(ctx.withValue(data)))
  }

  def andThen[U](s: Stage[T, U]): Stage[I, U] = flatMap(_ => s)
  def ~>[U](s: Stage[T, U]): Stage[I, U]      = andThen(s)

  def run(ctx: Stage.Context[I]): Stage.Result[T]
}

object Stage {
  type Result[A] = Try[A]

  abstract class Context[+I](val spark: SparkSession, val date: DateInterval, val value: I) {
    def withValue[U](value: U): Context[U]
    def run[T](stage: Stage[I, T], result: => T): Result[T] = Try(result)
  }

  object Context {
    def apply(spark: SparkSession, date: DateInterval): Context[SparkSession] =
      new SimpleContext(spark, date, spark)
  }

  class SimpleContext[+I](spark: SparkSession, date: DateInterval, value: I)
      extends Context[I](spark, date, value) {
    override def withValue[U](value: U) = new SimpleContext(spark, date, value)
  }

  def apply[S, T](stageName: String)(f: Stage.Context[S] => T): Stage[S, T] = new Stage[S, T] {
    override def name                       = stageName
    override def run(ctx: Stage.Context[S]) = ctx.run(this, f(ctx))
  }

  /**
   * Read data from a data source.
   *
   * May be used anywhere in a for-expression to read from a data source.
   */
  def source[T](name: String)(read: Stage.Context[SparkSession] => Dataset[T]) =
    Stage(name) { ctx: Stage.Context[_] =>
      read(ctx.withValue(ctx.spark))
    }

  def transform[S, T](name: String)(transform: Stage.Context[Dataset[S]] => Dataset[T]) =
    Stage(name)(transform)

  def sink[T](name: String)(write: Stage.Context[Dataset[T]] => Unit) =
    Stage(name)((ctx: Stage.Context[Dataset[T]]) => { write(ctx); ctx.value })

  def sequence[S, T](stages: Seq[Stage[S, T]]): Stage[S, Seq[T]] = new Stage[S, Seq[T]] {
    override def name = "sequence"
    override def run(ctx: Stage.Context[S]): Stage.Result[Seq[T]] = {
      @scala.annotation.tailrec
      def iterate(stages: Seq[Stage[S, T]], results: Seq[T]): Stage.Result[Seq[T]] =
        stages match {
          case Seq() => Success(results)
          case Seq(stage, rest @ _*) =>
            stage.run(ctx) match {
              case Success(result)    => iterate(rest, results :+ result)
              case Failure(exception) => Failure(exception)
            }
        }

      iterate(stages, Seq.empty)
    }
  }

  case class SequenceAllException[S, T](failures: Seq[(Stage[S, T], Throwable)]) extends Exception {
    override def getMessage =
      failures
        .map {
          case (stage, failure) =>
            s"${stage.name} failed: (${failure.getClass.getName}) ${failure.getMessage}"
        }
        .mkString(s"${failures.size} stage(s) failed:\n - ", "\n - ", "")
  }

  /**
   * Combine multiple stages into a single stage which fails if any of them results
   * in a failure.
   */
  def sequenceAll[S, T](stages: Seq[Stage[S, T]]): Stage[S, Seq[T]] = new Stage[S, Seq[T]] {
    override def name = "sequenceAll"
    override def run(ctx: Stage.Context[S]): Stage.Result[Seq[T]] = {
      val results: Seq[(Stage[S, T], Stage.Result[T])] =
        stages.map(stage => stage -> stage.run(ctx))
      results.filter(_._2.isFailure) match {
        case Seq() => Success(results.map(_._2.get))
        case failures =>
          Failure(SequenceAllException(failures.map {
            case (stage, result) => stage -> result.failed.get
          }))
      }
    }
  }

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
