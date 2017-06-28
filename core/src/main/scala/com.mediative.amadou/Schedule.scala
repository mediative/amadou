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

/**
 * Mixin providing a DSL for defining schedules.
 *
 * If for example, we want to run a report against a cumulative data source
 * each day and must ensure that it runs the first of every month to generate
 * a report for all of the previous month. This schedule can be defined as:
 *
 * {{{
 * val schedule = today and monthly
 * }}}
 *
 * If we are only interested in "backfilling" jobs from a specific date use
 * a `where` clause, e.g.:
 *
 * {{{
 * val schedule = daily where (_ >= Date(2016, 4, 12)
 * }}}
 */
trait ScheduleDsl {
  def today     = Schedule(Day.today)
  def daily     = Schedule.iterate(Day.today)
  def monthly   = Schedule.iterate(Month.today)
  def quarterly = Schedule.iterate(Quarter.today)
  def yearly    = Schedule.iterate(Year.today)
}

/**
 * A schedule is a sequence of monotonic decreasing dates. It can be
 * composed with other schedules to form more complex execution plans.
 *
 * Generating a list of days for a daily schedule:
 * {{{
 * scala> Schedule.iterate(Day(2016, 7, 2)).take(3).toList
 * res2: List[DateInterval] = List(2016-07-02, 2016-07-01, 2016-06-30)
 * }}}
 *
 * Generate a list of days for a monthly schedule:
 * {{{
 * scala> Schedule.iterate(Month(2016, 3)).where(Year(2016).<=).toList
 * res1: List[DateInterval] = List(2016-03, 2016-02, 2016-01)
 * }}}
 */
case class Schedule(dates: Stream[DateInterval]) extends Traversable[DateInterval] {
  override final def foreach[U](f: DateInterval => U): Unit = {
    @scala.annotation.tailrec
    def ensureMonotonicDecreasingOrder(stream: Stream[DateInterval]): Unit =
      if (stream.nonEmpty) {
        val head = stream.head
        f(head)
        val tail = stream.tail.dropWhile(_ >= head)
        ensureMonotonicDecreasingOrder(tail)
      }

    ensureMonotonicDecreasingOrder(dates)
  }

  /**
   * Combine two date streams.
   */
  def and(that: Schedule): Schedule = Schedule(dates #::: that.dates)

  /**
   * Only includes dates newer than a given date.
   */
  def where(predicate: DateInterval => Boolean): Schedule = Schedule(dates.takeWhile(predicate))
}

object Schedule {
  val empty                               = Schedule(Stream.empty[DateInterval])
  def apply(date: DateInterval): Schedule = Schedule(Stream(date))
  def iterate(interval: DateInterval): Schedule =
    Schedule(Stream.iterate(interval)(_.prev))
}
