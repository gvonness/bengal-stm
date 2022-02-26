/*
 * Copyright 2020-2021 Greg von Nessi
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

import org.scalatest.flatspec.AnyFlatSpec

class StmApiSpec extends AnyFlatSpec {

  "pure" should "yield argument value" in new StmRuntimeFixture {
    import stm._

    val pureTxn: Txn[String] =
      stm.pure("foo")

    assertResult("foo") {
      pureTxn.commit.unsafeRunSync()
    }
  }

  "raiseError" should "throw an error when run" in new StmRuntimeFixture {
    import stm._

    val errorTxn: Txn[Unit] =
      stm.abort(new RuntimeException("test error"))

    assertThrows[RuntimeException] {
      errorTxn.commit.unsafeRunSync()
    }
  }

  "handleErrorWith" should "recover from an error" in new StmRuntimeFixture {
    import stm._

    val mockError = new RuntimeException("mock error")

    assertResult(mockError.getMessage) {
      stm
        .abort(mockError)
        .flatMap(_ => stm.pure("test"))
        .handleErrorWith(ex => stm.pure(ex.getMessage))
        .commit
        .unsafeRunSync()
    }
  }

  it should "bypass mutations from the error transaction" in new StmRuntimeFixture {
    import stm._

    val baseMap = Map("foo" -> 42, "bar" -> 27, "baz" -> 18)

    val tVarMap: TxnVarMap[String, Int] = TxnVarMap.of(baseMap).unsafeRunSync()

    assertResult(Some(44)) {
      (for {
        result <- tVarMap.get("foo")
        _      <- tVarMap.modify("foo", _ + 3)
        _      <- stm.abort(new RuntimeException("fake exception"))
        _      <- tVarMap.modify("foo", _ + 2)
      } yield result).handleErrorWith { _ =>
        for {
          _      <- tVarMap.modify("foo", _ + 2)
          result <- tVarMap.get("foo")
        } yield result
      }.commit.unsafeRunSync()
    }
  }
}
