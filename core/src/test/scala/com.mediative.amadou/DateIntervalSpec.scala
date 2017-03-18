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

import org.scalatest._

class DateIntervalSpec extends FreeSpec {

  "Day" - {
    "should print date" in {
      assert(Day(1999, 12, 31).toString == "1999-12-31")
      assert(Day(2016, 6, 5).toString == "2016-06-05")
    }

    "should parse date" in {
      val date = Day.parse("2016-06-05")
      assert(date == Some(Day(2016, 6, 5)))
    }

    "should have next/prev" in {
      val dt = Day(2016, 6, 5)
      assert(dt.prev < dt)
      assert(dt < dt.next)
      assert(dt.prev == Day(2016, 6, 4))
      assert(dt.next == Day(2016, 6, 6))

      val dt2 = Day(1999, 12, 31)
      assert(dt2.prev == Day(1999, 12, 30))
      assert(dt2.next == Day(2000, 1, 1))
    }

    "should have iterator for dates" in {
      val dt = Day(2016, 6, 5)
      assert(dt.by(Day).size == 1)
    }
  }

  "Week" - {
    "should print date" in {
      assert(Week(1999, 52).toString == "1999-W52")
      assert(Week(2016, 1).toString == "2016-W01")
    }

    "should parse date" in {
      assert(Week.parse("2016-W01") == Some(Week(2016, 1)))
      assert(Week.parse("1999-W51") == Some(Week(1999, 51)))
    }

    "should have next/prev" in {
      val dt = Week(2016, 6)
      assert(dt.prev < dt)
      assert(dt < dt.next)
      assert(dt.prev == Week(2016, 5))
      assert(dt.next == Week(2016, 7))

      val dt2 = Week(1999, 52)
      assert(dt2.prev == Week(1999, 51))
      assert(dt2.next == Week(2000, 1))
    }

    "should have iterator for dates" in {
      val dt = Week(2016, 6)
      assert(dt.by(Day).size == 7)
    }
  }

  "Month" - {
    "should print date" in {
      assert(Month(1999, 12).toString == "1999-12")
      assert(Month(2016, 6).toString == "2016-06")
    }

    "should parse date" in {
      val dt = Month.parse("2016-06")
      assert(dt == Some(Month(2016, 6)))
    }

    "should have next/prev" in {
      val dt = Month(2016, 6)
      assert(dt.prev < dt)
      assert(dt < dt.next)
      assert(dt.prev == Month(2016, 5))
      assert(dt.next == Month(2016, 7))

      val dt2 = Month(1999, 12)
      assert(dt2.prev == Month(1999, 11))
      assert(dt2.next == Month(2000, 1))
    }

    "should have iterator for dates" in {
      val dt = Month(2016, 6)
      assert(dt.by(Day).size == 30)

      val dt2 = Month(2016, 2)
      assert(dt2.by(Day).size == 29)

      val dt3 = Month(1999, 2)
      assert(dt3.by(Day).size == 28)
    }
  }

  "Quarter" - {
    "should truncate month" in {
      assert(Month(Quarter(2016, Quarter.Q1)) == Month(2016, 1))
      assert(Month(Quarter(2016, Quarter.Q2)) == Month(2016, 4))
      assert(Month(Quarter(2016, Quarter.Q3)) == Month(2016, 7))
      assert(Month(Quarter(2016, Quarter.Q4)) == Month(2016, 10))

      assert(Month(Quarter(Month(2016, 2))) == Month(2016, 1))
      assert(Month(Quarter(Month(2016, 3))) == Month(2016, 1))
      assert(Month(Quarter(Month(2016, 12))) == Month(2016, 10))
    }

    "should print date" in {
      assert(Quarter(1999, Quarter.Q1).toString == "1999-Q1")
      assert(Quarter(2016, Quarter.Q3).toString == "2016-Q3")
    }

    "should parse date" in {
      val dt = Quarter.parse("2016-Q4")
      assert(dt == Some(Quarter(2016, Quarter.Q4)))
      assert(Month(dt.get) == Month(2016, 10))
    }

    "should have next/prev" in {
      val dt = Quarter(2014, Quarter.Q1)
      assert(dt.prev < dt)
      assert(dt < dt.next)
      assert(dt.prev == Quarter(2013, Quarter.Q4))
      assert(dt.next == Quarter(2014, Quarter.Q2))

      val dt2 = Quarter(1999, Quarter.Q4)
      assert(dt2.prev == Quarter(1999, Quarter.Q3))
      assert(dt2.next == Quarter(2000, Quarter.Q1))
    }

    "should have iterator for dates" in {
      val dt = Quarter(2016, Quarter.Q1)
      assert(dt.by(Day).size == 91)

      val dt2 = Quarter(1999, Quarter.Q1)
      assert(dt2.by(Day).size == 90)

      val dt3 = Quarter(1999, Quarter.Q4)
      assert(dt3.by(Day).size == 92)
    }
  }

  "Year" - {
    "should print date" in {
      assert(Year(1999).toString == "1999")
      assert(Year(2016).toString == "2016")
    }

    "should parse date" in {
      val dt = Year.parse("2016")
      assert(dt == Some(Year(2016)))
    }

    "should have next/prev" in {
      val dt = Year(2016)
      assert(dt.prev < dt)
      assert(dt < dt.next)
      assert(dt.prev == Year(2015))
      assert(dt.next == Year(2017))

      val dt2 = Year(1999)
      assert(dt2.prev == Year(1998))
      assert(dt2.next == Year(2000))
    }

    "should have iterator for dates" in {
      val dt = Year(2016)
      assert(dt.by(Day).size == 366)

      val dt2 = Year(1999)
      assert(dt2.by(Day).size == 365)

      assert(Year(1999).by(Quarter).size == 4)
    }
  }

}
