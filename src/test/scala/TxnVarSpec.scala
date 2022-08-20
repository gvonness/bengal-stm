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

import bengal.stm.model._
import bengal.stm.syntax.all._

import cats.effect.IO
import org.scalatest.flatspec.AnyFlatSpec

class TxnVarSpec extends AnyFlatSpec {
  "TxnVar.get" should "return the value of a transactional variable" in new StmRuntimeFixture {
    val tVar: TxnVar[IO, Int] = TxnVar.of(123).unsafeRunSync()

    assertResult(123) {
      tVar.get.commit.unsafeRunSync()
    }
  }

  "TxnVar.set" should "update the value of a transactional variable" in new StmRuntimeFixture {

    val tVar: TxnVar[IO, Int] = TxnVar.of(123).unsafeRunSync()

    tVar.set(2718).commit.unsafeRunSync()

    assertResult(2718) {
      tVar.get.commit.unsafeRunSync()
    }
  }

  "TxnVar.modify" should "modify the value of a transactional variable" in new StmRuntimeFixture {

    val tVar: TxnVar[IO, String] = TxnVar.of("foo").unsafeRunSync()

    tVar.modify(_ + "bar").commit.unsafeRunSync()

    assertResult("foobar") {
      tVar.get.commit.unsafeRunSync()
    }
  }
}
