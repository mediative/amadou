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

import org.scalatest.{WordSpec, Matchers}
import java.io.IOException

object ManagedIOSpec {
  class IO extends AutoCloseable {
    var closed = false
    def fail   = throw new IOException("IO failed")
    override def close(): Unit =
      closed = true
  }

  object IO {
    def openFail(): IO =
      throw new IOException("IO open failed")
  }

  class IOWithFailingClose extends IO {
    override def close(): Unit = {
      closed = true
      throw new IOException("IO close failed")
    }
  }
}

class ManagedIOSpec extends WordSpec with Matchers {
  import ManagedIOSpec._

  "ManagedIO" should {
    "call close on happy path" in {
      val io0 = new IO
      val io1 = new IO
      for {
        x0 <- ManagedIO(io0)
        x1 <- ManagedIO(io1)
      } {
        x0.closed shouldBe false
        x1.closed shouldBe false
        ()
      }

      io0.closed shouldBe true
      io1.closed shouldBe true
    }

    "call close when open fails" in {
      val io0 = new IO

      the[IOException] thrownBy {
        for {
          x0 <- ManagedIO(io0)
          x1 <- ManagedIO(IO.openFail())
        } ()
      } should have message "IO open failed"

      io0.closed shouldBe true
    }

    "not call close for inner open calls" in {
      the[IOException] thrownBy {
        for {
          x0 <- ManagedIO(IO.openFail())
        } fail("Should not be called")
      } should have message "IO open failed"
    }

    "call close with nested failures" in {
      val io0 = new IO
      val io1 = new IO

      the[IOException] thrownBy {
        for {
          x0 <- ManagedIO(io0)
          x1 <- ManagedIO(io1)
        } {
          x1.fail
        }
      } should have message "IO failed"

      io0.closed shouldBe true
      io1.closed shouldBe true
    }

    "propagate close failures" in {
      val io0 = new IO
      val io1 = new IOWithFailingClose

      the[IOException] thrownBy {
        for {
          x0 <- ManagedIO(io0)
          x1 <- ManagedIO(io1)
        } {
          ()
        }
      } should have message "IO close failed"

      io0.closed shouldBe true
      io1.closed shouldBe true
    }
  }
}
