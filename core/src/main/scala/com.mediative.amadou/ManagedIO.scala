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

import scala.util.Try

/**
 * Manage auto closeable resources.
 */
case class ManagedIO[T <: AutoCloseable](t: Try[T]) {
  def flatMap[U <: AutoCloseable](f: T => ManagedIO[U]): ManagedIO[U] = run(f)
  def map[U](f: T => U): U                                            = run(f)
  def foreach(f: T => Unit): Unit                                     = run(f)

  private def run[U](f: T => U): U = {
    val result = t.flatMap(v => Try(f(v)))
    val close  = t.flatMap(v => Try(v.close))
    if (result.isSuccess)
      close.get
    result.get
  }
}

object ManagedIO {
  def apply[T <: AutoCloseable](open: => T): ManagedIO[T] =
    ManagedIO(Try(open))
}
