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

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import scala.util.Try

/**
 * Base class for date intervals.
 *
 * The interval is represented as the half open range between `from` and `to`.
 * For instance, May 2014 is represented as `from = 2014-05-01`, `to = 2014-06-01`.
 *
 * Example:
 * {{{
 * scala> Seq(Day(2016, 8, 11), Week(2016, 32), Month(2016, 8), Quarter(2016, Quarter.Q3), Year(2016))
 * res1: Seq[DateInterval] = List(2016-08-11, 2016-W32, 2016-08, 2016-Q3, 2016)
 * }}}
 */
sealed abstract class DateInterval(
    val from: Long,
    val interval: DateIntervalType,
    val toOpt: Option[DateInterval] = None)
    extends Ordered[DateInterval] {

  def end = toOpt.getOrElse(next)

  /**
   * Define a custom interval.
   *
   * Example:
   * {{{
   * scala> Month(2016, 8) to Day(2016, 8, 29)
   * res1: DateInterval = 2016-08:2016-08-29
   * scala> (Day(2016, 8, 11) to Day(2016, 8, 29)).by(Day).size
   * res2: Int = 18
   * }}}
   */
  def to(other: DateInterval): DateInterval =
    if (toOpt.forall(_ < other))
      interval.newBuilder.from(from).to(other).build
    else
      this

  def +(delta: Int): DateInterval = interval.newBuilder.from(from).add(delta).build
  def -(delta: Int): DateInterval = this.+(-delta)

  override def equals(other: Any): Boolean = other match {
    case that: DateInterval =>
      interval == that.interval && toOpt == that.toOpt && compare(that) == 0
    case _ => false
  }

  override def compare(that: DateInterval) = from.compareTo(that.from)

  override def toString =
    interval.defaultFormatter.format(asDate) + toOpt.fold("")(":" + _.toString)

  def format(dateFormat: String): String = {
    val formatter = interval.formatter(dateFormat)
    formatter.format(asDate) + toOpt.fold("")(":" + _.toString)
  }

  def asDate      = new Date(from)
  def asTimestamp = new Timestamp(from)

  def contains(date: DateInterval): Boolean =
    this <= date && date < end

  /**
   * Iterate over all days in this date interval.
   *
   * Example:
   * {{{
   * scala> Week(2016, 11).by(Day).size
   * res1: Int = 7
   * scala> Week(2016, 11).by(Day).toList
   * res2: List[DateInterval] = List(2016-03-14, 2016-03-15, 2016-03-16, 2016-03-17, 2016-03-18, 2016-03-19, 2016-03-20)
   * }}}
   */
  def by(interval: DateIntervalType): Traversable[DateInterval] = {
    var d = interval.apply(this)

    Stream.iterate(d)(_.next).takeWhile(_.from < end.from)
  }

  /**
   * Returns the preceding corresponding date interval (eg. May -> April).
   *
   * Example:
   * {{{
   * scala> Month(2016, 5).prev
   * res1: DateInterval = 2016-04
   * }}}
   */
  def prev: DateInterval = this - 1

  /**
   * Returns the subsequent corresponding date interval (eg. 2014 -> 2015).
   *
   * Example:
   * {{{
   * scala> Year(2014).next
   * res1: DateInterval = 2015
   * }}}
   */
  def next: DateInterval = this + 1
}

object DateInterval {
  val UTC = TimeZone.getTimeZone("UTC")
}

/**
 * Base class for specific date intervals
 */
sealed abstract class DateIntervalType(val calendarField: Int, val dateFormat: String) {

  /**
   * Create a date interval with a different type and truncate lower
   * resolution date fields accordingly.
   *
   * Example:
   * {{{
   * scala> Day(1431000000000L)
   * res1: DateInterval = 2015-05-07
   * scala> Week(Day(1431000000000L))
   * res2: DateInterval = 2015-W19
   * scala> Month(Day(1431000000000L))
   * res3: DateInterval = 2015-05
   * scala> Quarter(1431000000000L)
   * res4: DateInterval = 2015-Q2
   * scala> Year(Day(1431000000000L))
   * res5: DateInterval = 2015
   * scala> Year(Day(1431000000000L)).format("yyyy-MM-dd HH:mm ZZ")
   * res6: String = 2015-01-01 00:00 +0000
   * }}}
   */
  def apply(date: DateInterval): DateInterval =
    apply(date.from)

  /**
   * Represent the interval for a given timestamp using the specified
   * interval type.
   *
   * This will truncate lower resolution date fields to their initial
   * value, for example for months the day of month field is set to 1.
   *
   * Example:
   * {{{
   * scala> Day(1431000000000L).format("yyyy-MM-dd HH:mm ZZ")
   * res1: String = 2015-05-07 00:00 +0000
   * scala> Month(1431000000000L).format("yyyy-MM-dd HH:mm ZZ")
   * res2: String = 2015-05-01 00:00 +0000
   * scala> Year(1431000000000L).format("yyyy-MM-dd HH:mm ZZ")
   * res3: String = 2015-01-01 00:00 +0000
   * }}}
   */
  def apply(timestamp: Long): DateInterval =
    newBuilder.from(timestamp).build

  /**
   * Represent the interval for today using the specified interval type.
   *
   * This will truncate lower resolution date fields to their initial
   * value, for example for months the day of month field is set to 1.
   */
  def today = apply(System.currentTimeMillis())

  /**
   * Parse a timestamp into a date interval.
   *
   * Example:
   * {{{
   * scala> Day.parse("2015-05-07")
   * res1: Option[DateInterval] = Some(2015-05-07)
   * scala> Day.parse("2015-05-07").get.format("yyyy-MM-dd HH:mm ZZ")
   * res2: String = 2015-05-07 00:00 +0000
   * }}}
   */
  def parse(input: String): Option[DateInterval] =
    Try(defaultFormatter.parse(input)).map(date => newBuilder.from(date.getTime).build).toOption

  private[amadou] def formatter(pattern: String = dateFormat): SimpleDateFormat = {
    val dateFormat = new SimpleDateFormat(pattern)
    dateFormat.setCalendar(Builder.newCalendar)
    dateFormat
  }

  private[amadou] def defaultFormatter: SimpleDateFormat =
    formatter(dateFormat)

  private[amadou] def newBuilder(): Builder =
    new Builder()

  private class Instance(from: Long, toOpt: Option[DateInterval])
      extends DateInterval(from, DateIntervalType.this, toOpt)

  private[amadou] case class Builder(from: Long = 0, toOpt: Option[DateInterval] = None) {
    def update(f: Calendar => Unit): Builder = {
      val cal = Builder.newCalendar
      cal.setTimeInMillis(from)
      f(cal)
      copy(from = cal.getTimeInMillis)
    }

    def from(date: DateInterval): Builder = copy(from = date.from)
    def from(timestamp: Long): Builder    = copy(from = timestamp)
    def to(date: DateInterval): Builder   = copy(toOpt = Some(date))

    def add(delta: Int): Builder =
      DateIntervalType.this match {
        case Quarter => update(_.add(calendarField, delta * 3))
        case _       => update(_.add(calendarField, delta))
      }

    def set(year: Int, month: Int = 1, day: Int = 1): Builder = {
      require(1 <= month && month <= 12, "month must be between 1-12")
      update(_.set(year, month - 1, day))
    }

    def build(): DateInterval = {
      val cal = Builder.newCalendar
      cal.setTimeInMillis(from)
      val truncated = Builder.newCalendar
      DateIntervalType.this match {
        case Week =>
          truncated.setWeekDate(cal.getWeekYear(), cal.get(Calendar.WEEK_OF_YEAR), Week.Monday.id)

        case Quarter =>
          truncated.set(cal.get(Calendar.YEAR), (cal.get(Calendar.MONTH) / 3) * 3, 1)

        case _ =>
          Seq(Calendar.DAY_OF_MONTH, Calendar.MONTH, Calendar.YEAR)
            .dropWhile(_ != calendarField)
            .reverse
            .foreach(field => truncated.set(field, cal.get(field)))
      }

      new Instance(truncated.getTimeInMillis, toOpt)
    }
  }

  object Builder {
    def newCalendar: Calendar = {
      val cal = Calendar.getInstance(DateInterval.UTC)
      cal.clear()
      cal.setLenient(false)
      /*
       * Configure the calendar for ISO 8601 standard compatible settings:
       * http://docs.oracle.com/javase/7/docs/api/java/util/GregorianCalendar.html#week_and_year
       */
      cal.setFirstDayOfWeek(Week.Monday.id)
      cal.setMinimalDaysInFirstWeek(4)
      cal.setTimeZone(DateInterval.UTC)
      cal
    }
  }
}

object Day extends DateIntervalType(Calendar.DAY_OF_MONTH, "yyyy-MM-dd") {
  def apply(year: Int, month: Int, day: Int): DateInterval =
    newBuilder.set(year, month, day).build
}

/**
 * ISO 8601 week. Note that it has some counterintuitive behavior around new year.
 * For instance Monday 29 December 2008 is week 2009-W01, and Sunday 3 January 2010 is week 2009-W53-7
 * This example was taken from from http://en.wikipedia.org/wiki/ISO_8601#Week_dates:
 * {{{
 * scala> Week(Day(2008, 12, 29))
 * res1: DateInterval = 2009-W01
 * scala> Week(Day(2010, 1, 3))
 * res2: DateInterval = 2009-W53
 * scala> Week(2009, 53, dayOfWeek = Week.Sunday).by(Day).last
 * res3: DateInterval = 2010-01-03
 * }}}
 */
object Week extends DateIntervalType(Calendar.WEEK_OF_YEAR, "YYYY-'W'ww") {
  sealed trait WeekDay {
    def id: Int
  }
  case object Monday    extends WeekDay { val id = Calendar.MONDAY    }
  case object Tuesday   extends WeekDay { val id = Calendar.TUESDAY   }
  case object Wednesday extends WeekDay { val id = Calendar.WEDNESDAY }
  case object Thursday  extends WeekDay { val id = Calendar.THURSDAY  }
  case object Friday    extends WeekDay { val id = Calendar.FRIDAY    }
  case object Saturday  extends WeekDay { val id = Calendar.SATURDAY  }
  case object Sunday    extends WeekDay { val id = Calendar.SUNDAY    }

  def apply(year: Int, week: Int, dayOfWeek: WeekDay = Monday): DateInterval = {
    require(1 <= week && week <= 53, "week must be between 1-53")
    newBuilder.update(_.setWeekDate(year, week, dayOfWeek.id)).build
  }
}

object Month extends DateIntervalType(Calendar.MONTH, "yyyy-MM") {
  def apply(year: Int, month: Int): DateInterval = newBuilder.set(year, month).build
}

sealed abstract class Quarter(val month: Int)

/**
 * Quarters of the year.
 *
 * {{{
 * scala> Quarter(2017, Quarter.Q1)
 * res1: DateInterval = 2017-Q1
 * scala> Year(2017).by(Quarter).toList
 * res2: List[DateInterval] = List(2017-Q1, 2017-Q2, 2017-Q3, 2017-Q4)
 * scala> Quarter(2017, Quarter.Q1).by(Month).toList
 * res3: List[DateInterval] = List(2017-01, 2017-02, 2017-03)
 * scala> Quarter.parse("2008-Q4")
 * res4: Option[DateInterval] = Some(2008-Q4)
 * scala> Quarter(2017, Quarter.Q2).format("yyyy-MM-dd")
 * res5: String = 2017-04-01
 * }}}
 */
object Quarter extends DateIntervalType(Calendar.MONTH, "yyyy-MMM") {
  case object Q1 extends Quarter(month = 1)
  case object Q2 extends Quarter(month = 4)
  case object Q3 extends Quarter(month = 7)
  case object Q4 extends Quarter(month = 10)

  private val MonthSymbols = Array(Q1, Q2, Q3, Q4).flatMap(q => Array.fill(3)(q.toString))
  private def dateFormatSymbols = new java.text.DateFormatSymbols() {
    setMonths(MonthSymbols)
    setShortMonths(MonthSymbols)
  }

  override private[amadou] def defaultFormatter: SimpleDateFormat = {
    val format = super.formatter(dateFormat)
    format.setDateFormatSymbols(dateFormatSymbols)
    format
  }

  def apply(year: Int, quarter: Quarter): DateInterval =
    newBuilder.set(year, quarter.month).build
}

object Year extends DateIntervalType(Calendar.YEAR, "yyyy") {
  def apply(year: Int): DateInterval = newBuilder.set(year).build
}
