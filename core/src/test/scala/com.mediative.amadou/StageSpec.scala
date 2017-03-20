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
import org.apache.spark.sql.SparkSession
import scala.util.{ Success, Try }

object StageSpec {
  case class Raw(a: String, b: String)
  case class Clean(a: Long, b: Int)

  case class TestContext[+I](
    override val spark: SparkSession,
    override val date: DateInterval,
    override val value: I)
      extends Stage.Context(spark, date, value) { self =>

    override def withValue[U](u: U) = new TestContext(spark, date, u) {
      override protected var stageNames = self.stageNames
    }
    override def run[T](stage: Stage[I, T], result: => T): Stage.Result[T] = {
      stageNames += stage.name
      Try(result)
    }

    def stagesRun = List(stageNames: _*)
    protected var stageNames = scala.collection.mutable.ArrayBuffer[String]()
  }
}

class StageSpec extends FreeSpec with Matchers with SparkJobSuiteBase {

  import StageSpec._

  val InitStage = Stage[Int, Int]("init")(_.value)
  val ToStringStage = Stage[Any, String]("toString")(_.value.toString)
  val ToIntStage = Stage[String, Int]("toInt")(_.value.toInt)
  val FailStage = Stage[Int, Int]("fail")(_.value / 0)

  "Stage" - {
    "name" - {
      "from string" in {
        InitStage.name should be("init")
      }

      "from scala.Symbol" in {
        'NameFromSymbol.stage[Int, Int](_ => 1).name should be("NameFromSymbol")
      }
    }

    "run" - {
      "succeeding operation" in {
        val ctx = TestContext(spark, Day.today, 1)
        InitStage.run(ctx) should be(Success(1))
        ctx.stagesRun should be("init" :: Nil)
      }

      "failing operation" in {
        val ctx = TestContext(spark, Day.today, 1)
        the[ArithmeticException] thrownBy FailStage.run(ctx).get should have message "/ by zero"
        ctx.stagesRun should be("fail" :: Nil)
      }
    }

    "map" - {
      "succeeding operation" in {
        val ctx = TestContext(spark, Day.today, 42)
        InitStage.map(_ + 1).run(ctx) should be(Success(43))
        ctx.stagesRun should be("init" :: Nil)
      }

      "multiple" in {
        val ctx = TestContext(spark, Day.today, 42)
        InitStage.map(_ + 1).map(_ + 1).run(ctx) should be(Success(44))
        ctx.stagesRun should be("init" :: Nil)
      }

      "failing operation" in {
        val ctx = TestContext(spark, Day.today, 1)
        the[ArithmeticException] thrownBy InitStage.map(_ / 0).run(ctx).get should have message "/ by zero"
        ctx.stagesRun should be("init" :: Nil)
      }

      "failing operation with unreachable mapping" in {
        val ctx = TestContext(spark, Day.today, 1)
        the[ArithmeticException] thrownBy InitStage.map(_ / 0).map(_ + 1).run(ctx).get should have message "/ by zero"
        ctx.stagesRun should be("init" :: Nil)
      }
    }

    "flatMap" - {
      "succeeding operation" in {
        val ctx = TestContext(spark, Day.today, 42)
        InitStage.flatMap(_ => ToStringStage).run(ctx) should be(Success("42"))
        ctx.stagesRun should be("init" :: "toString" :: Nil)
      }

      "multiple" in {
        val ctx = TestContext(spark, Day.today, 42)
        val stages = for {
          init <- InitStage
          toString <- ToStringStage
          value <- 'stringToInt.stage[String, Int](_.value.toInt)
        } yield value

        stages.run(ctx) should be(Success(42))
        ctx.stagesRun should be("init" :: "toString" :: "stringToInt" :: Nil)
      }

      "failing operation" in {
        val ctx = TestContext(spark, Day.today, 42)
        val stages = for {
          init <- InitStage
          value <- FailStage
          toString <- ToStringStage
        } yield ()

        the[ArithmeticException] thrownBy stages.run(ctx).get should have message "/ by zero"
        ctx.stagesRun should be("init" :: "fail" :: Nil)
      }

      "failing last operation" in {
        val ctx = TestContext(spark, Day.today, 42)
        val stages = for {
          init <- InitStage
          toString <- ToStringStage
          toInt <- ToIntStage
          value <- FailStage
        } yield ()

        the[ArithmeticException] thrownBy stages.run(ctx).get should have message "/ by zero"
        ctx.stagesRun should be("init" :: "toString" :: "toInt" :: "fail" :: Nil)
      }
    }

    "andThen and ~>" - {
      "succeeding operation" in {
        val ctx = TestContext(spark, Day.today, 42)
        (InitStage andThen ToStringStage).run(ctx) should be(Success("42"))
        ctx.stagesRun should be("init" :: "toString" :: Nil)
      }

      "succeeding operation using ~>" in {
        val ctx2 = TestContext(spark, Day.today, 42)
        (InitStage ~> ToStringStage ~> ToIntStage).run(ctx2) should be(Success(42))
        ctx2.stagesRun should be("init" :: "toString" :: "toInt" :: Nil)
      }

      "failing operation" in {
        val ctx = TestContext(spark, Day.today, 42)
        val stages: Stage[Int, String] = InitStage ~> FailStage ~> ToStringStage

        the[ArithmeticException] thrownBy stages.run(ctx).get should have message "/ by zero"
        ctx.stagesRun should be("init" :: "fail" :: Nil)
      }

      "failing last operation" in {
        val ctx = TestContext(spark, Day.today, 42)
        val stages: Stage[Int, Int] = InitStage ~> ToStringStage ~> ToIntStage ~> FailStage

        the[ArithmeticException] thrownBy stages.run(ctx).get should have message "/ by zero"
        ctx.stagesRun should be("init" :: "toString" :: "toInt" :: "fail" :: Nil)
      }
    }

    "identity" - {
      "passes original value" in {
        val ctx = TestContext(spark, Day.today, 42)
        val empty: Stage[Int, Int] = Stage.identity

        empty.run(ctx) should be(Success(42))
        ctx.stagesRun should be("identity" :: Nil)
      }

      "map run via the identity stage" in {
        val ctx = TestContext(spark, Day.today, 42)
        Stage.identity[Int].map(_.toDouble).run(ctx) should be(Success(42.0))
        ctx.stagesRun should be("identity" :: Nil)
      }

      "flatMap ignores the identity stage" in {
        val ctx = TestContext(spark, Day.today, 42)
        Stage.identity[Int].flatMap(_ => ToStringStage).run(ctx) should be(Success("42"))
        ctx.stagesRun should be("toString" :: Nil)
      }
    }
  }
}
