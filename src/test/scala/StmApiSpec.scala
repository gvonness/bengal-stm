/*
 * Copyright 2020-2022 Greg von Nessi
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

package ai.entrolution

import bengal.stm.STM
import bengal.stm.model._
import bengal.stm.syntax.all._

import cats.effect.IO
import org.scalatest.flatspec.AnyFlatSpec

class StmApiSpec extends AnyFlatSpec {

  "delay" should "yield argument value" in new StmRuntimeFixture {

    val pureTxn: Txn[String] =
      STM[IO].delay("foo")

    assertResult("foo") {
      pureTxn.commit.unsafeRunSync()
    }
  }

  "pure" should "yield argument value" in new StmRuntimeFixture {

    val pureTxn: Txn[String] =
      STM[IO].pure("foo")

    assertResult("foo") {
      pureTxn.commit.unsafeRunSync()
    }
  }

  "raiseError" should "throw an error when run" in new StmRuntimeFixture {

    val errorTxn: Txn[Unit] =
      STM[IO].abort(new RuntimeException("test error"))

    assertThrows[RuntimeException] {
      errorTxn.commit.unsafeRunSync()
    }
  }

  "handleErrorWith" should "recover from an error" in new StmRuntimeFixture {
    val mockError = new RuntimeException("mock error")

    assertResult(mockError.getMessage) {
      stm
        .abort(mockError)
        .flatMap(_ => STM[IO].delay("test"))
        .handleErrorWith(ex => STM[IO].delay(ex.getMessage))
        .commit
        .unsafeRunSync()
    }
  }

  it should "bypass mutations from the error transaction" in new StmRuntimeFixture {

    val baseMap = Map("foo" -> 42, "bar" -> 27, "baz" -> 18)

    val tVarMap: TxnVarMap[IO, String, Int] =
      TxnVarMap.of(baseMap).unsafeRunSync()

    assertResult(Some(44)) {
      (for {
        result <- tVarMap.get("foo")
        _      <- tVarMap.modify("foo", _ + 3)
        _      <- STM[IO].abort(new RuntimeException("fake exception"))
        _      <- tVarMap.modify("foo", _ + 2)
      } yield result).handleErrorWith { _ =>
        for {
          _      <- tVarMap.modify("foo", _ + 2)
          result <- tVarMap.get("foo")
        } yield result
      }.commit.unsafeRunSync()
    }
  }

  "waitFor" should "should complete when predicate is satisfied" in new StmRuntimeFixture {

    val tVar: TxnVar[IO, Int] = TxnVar.of(1).unsafeRunSync()

    val program1: Txn[Int] = for {
      result <- tVar.get
      _      <- STM[IO].waitFor(result > 3)
    } yield result

    val program2: Txn[Unit] =
      tVar.set(5)

    assertResult(5) {
      (for {
        resFib <- program1.commit.start
        _      <- program2.commit.start
        result <- resFib.joinWithNever
      } yield result).unsafeRunSync()
    }
  }
}
