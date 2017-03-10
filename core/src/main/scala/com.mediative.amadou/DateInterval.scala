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

import java.sql.{ Date, Timestamp }
import java.text.{ DateFormat, SimpleDateFormat }
import java.util.{ Calendar, TimeZone }

import scala.util.Try

/**
 * Base class for date intervals.
 *
 * The interval is represented as the half open range between `from` and `to`.
 * For instance, May 2014 is represented as `from = 2014-05-01`, `to = 2014-06-01`.
 *
 * Example:
 * {{{
 * scala> Seq(Day(2016, 8, 11), Month(2016, 8), Year(2016))
 * res1: Seq[DateInterval] = List(2016-08-11, 2016-08, 2016)
 * }}}
 */
case class DateInterval(
    timestamp: Long,
    interval: DateIntervalType,
    toOpt: Option[DateInterval] = None) extends Ordered[DateInterval] {

  val from = interval.truncate(timestamp)

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
    copy(toOpt = Some(other))

  def +(delta: Int): DateInterval = DateInterval(interval) { cal =>
    cal.setTimeInMillis(from.getTime)
    cal.add(interval.calendarField, delta)
  }

  def -(delta: Int): DateInterval = this.+(-delta)

  override def equals(other: Any): Boolean = other match {
    case that: DateInterval => interval == that.interval && toOpt == that.toOpt && compare(that) == 0
    case _ => false
  }

  override def compare(that: DateInterval) = from.compareTo(that.from)

  override def toString = format(interval.dateFormat)

  def format(dateFormat: String): String = {
    val formatter = interval.formatter(dateFormat)
    formatter.format(from) + toOpt.fold("")(":" + _.toString)
  }

  def toTimestamp: Timestamp = new Timestamp(from.getTime)

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

    Stream.iterate(d)(_.next).takeWhile(_.from.before(end.from))
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

  private[amadou] def createUTCCalendar(): Calendar = {
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

  private[amadou] def apply(interval: DateIntervalType)(f: Calendar => Unit): DateInterval = {
    val cal = createUTCCalendar()
    f(cal)
    DateInterval(cal.getTimeInMillis, interval)
  }
}

/**
 * Base class for specific date intervals
 */
sealed abstract class DateIntervalType(val calendarField: Int, val dateFormat: String) {
  protected def create(year: Int, month: Int = 1, day: Int = 1): DateInterval = DateInterval(this) { cal =>
    require(1 <= month && month <= 12, "month must be between 1-12")
    cal.set(year, month - 1, day)
  }

  def truncate(timestamp: Long): Date = {
    val orig = DateInterval.createUTCCalendar()
    orig.setTimeInMillis(timestamp)
    val cal = DateInterval.createUTCCalendar()
    truncate(orig, cal)
    new Date(cal.getTimeInMillis)
  }

  protected def truncate(orig: Calendar, cal: Calendar): Unit = {
    val fields = Seq(Calendar.DAY_OF_MONTH, Calendar.MONTH, Calendar.YEAR).dropWhile(_ != calendarField)
    for (field <- fields.reverse)
      cal.set(field, orig.get(field))
  }

  /**
   * Create a date interval with a different type and truncate lower
   * resolution date fields accordingly.
   *
   * Example:
   * {{{
   * scala> Day(1431000000000L)
   * res1: DateInterval = 2015-05-07
   * scala> Month(Day(1431000000000L))
   * res2: DateInterval = 2015-05
   * scala> Year(Day(1431000000000L))
   * res3: DateInterval = 2015
   * scala> Year(Day(1431000000000L)).format("yyyy-MM-dd HH:mm ZZ")
   * res4: String = 2015-01-01 00:00 +0000
   * }}}
   */
  def apply(date: DateInterval): DateInterval =
    DateInterval(date.from.getTime, this)

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
    DateInterval(timestamp, this)

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
    Try(formatter(dateFormat).parse(input)).map(date => DateInterval(date.getTime, this)).toOption

  def formatter(pattern: String = dateFormat): DateFormat = {
    val cal = DateInterval.createUTCCalendar()
    val dateFormat = new SimpleDateFormat(pattern)
    dateFormat.setCalendar(cal)
    dateFormat
  }
}

object Day extends DateIntervalType(Calendar.DAY_OF_MONTH, "yyyy-MM-dd") {
  def apply(year: Int, month: Int, day: Int): DateInterval =
    create(year, month, day)
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
  case object Monday extends WeekDay { val id = Calendar.MONDAY }
  case object Tuesday extends WeekDay { val id = Calendar.TUESDAY }
  case object Wednesday extends WeekDay { val id = Calendar.WEDNESDAY }
  case object Thursday extends WeekDay { val id = Calendar.THURSDAY }
  case object Friday extends WeekDay { val id = Calendar.FRIDAY }
  case object Saturday extends WeekDay { val id = Calendar.SATURDAY }
  case object Sunday extends WeekDay { val id = Calendar.SUNDAY }

  def apply(year: Int, week: Int, dayOfWeek: WeekDay = Monday): DateInterval = DateInterval(this) { cal =>
    require(1 <= week && week <= 53, "week must be between 1-53")
    cal.setWeekDate(year, week, dayOfWeek.id)
  }

  override protected def truncate(orig: Calendar, cal: Calendar): Unit = {
    cal.setWeekDate(
      orig.getWeekYear(),
      orig.get(Calendar.WEEK_OF_YEAR),
      Monday.id
    )
  }
}

object Month extends DateIntervalType(Calendar.MONTH, "yyyy-MM") {
  def apply(year: Int, month: Int): DateInterval = create(year, month)
}

object Year extends DateIntervalType(Calendar.YEAR, "yyyy") {
  def apply(year: Int): DateInterval = create(year)
}
