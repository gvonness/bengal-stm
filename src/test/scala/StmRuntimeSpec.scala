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

import scala.collection.immutable.Queue

class StmRuntimeSpec extends AnyFlatSpec {
  "commit" should "correctly execute multiple programs" in new StmRuntimeFixture {
    val txnVarTest: TxnVar[IO, Int] = TxnVar.of(11).unsafeRunSync()

    val txnVarMapTest: TxnVarMap[IO, String, Int] =
      TxnVarMap.of(Map("foo" -> 5, "bar" -> 1)).unsafeRunSync()

    val program1: Txn[Int] = for {
      v0 <- txnVarTest.get // 11
      v1 <- txnVarMapTest.get("foo") // 5
      _  <- txnVarMapTest.set("foobaz", 4)
      v4 <- txnVarMapTest.get("foobaz") // 4
      _  <- txnVarMapTest.set(Map("foobar" -> -10, "barbaz" -> 77))
      _  <- STM[IO].waitFor(v0 > 2)
      _  <- txnVarTest.modify(_ + 5)
      v2 <- txnVarTest.get // 16
      v3 <- txnVarTest.get // 16
      v5 <- txnVarMapTest.get("barbaz") // 77
      _  <- txnVarMapTest.remove("barbaz")
      v6 <- txnVarMapTest.get // Map("foobar" -> -10)
      v7 <- txnVarMapTest.get("foo") // None
      v8 <- STM[IO].delay(12)
    } yield List[Option[Int]](
      Some(v0),            // 11
      v1,                  // Some(5)
      Some(v2),            // Some(16)
      Some(v3),            // Some(16)
      v4,                  // Some(4)
      v5,                  // Some(77)
      Some(v6.values.sum), // Some(-10)
      v7,                  // None
      Some(v8)             // Some(12)
    ).flatten.sum          // 131

    val program2: Txn[Int] = for {
      v0 <- txnVarTest.get // 16
      _  <- STM[IO].waitFor(v0 > 12)
      _  <- txnVarMapTest.set("foo", -33)
      _  <- txnVarTest.modify(_ + 5)
      v1 <- txnVarTest.get // 21
      v2 <- txnVarMapTest.get // Map("foo" -> -33, "foobar" -> -10)
      _  <- STM[IO].unit
    } yield v0 + v1 + v2.values.sum // -6

    assertResult(125) {
      (for {
        result2f <- program2.commit.start
        result1f <- program1.commit.start
        result2  <- result2f.joinWithNever // -6
        result1  <- result1f.joinWithNever // 131
      } yield result1 + result2).unsafeRunSync() // 125
    }
  }

  it should "correctly execute with transient evaluation errors in the static analysis" in new StmRuntimeFixture {

    val txnVarQueue: TxnVar[IO, Queue[Int]] =
      TxnVar.of(Queue[Int]()).unsafeRunSync()

    val txnVar: TxnVar[IO, Int] =
      TxnVar.of(0).unsafeRunSync()

    val program1: Txn[Unit] = for {
      queueResult <- txnVarQueue.get
      _           <- STM[IO].waitFor(queueResult.nonEmpty)
      result      <- STM[IO].delay(queueResult.dequeue)
      _           <- txnVarQueue.set(result._2)
      _           <- txnVar.set(result._1)
    } yield ()

    val program2: Txn[Unit] = for {
      _ <- txnVarQueue.modify(_.enqueue(27))
      _ <- txnVarQueue.modify(_.enqueue(18))
      _ <- txnVarQueue.modify(_.enqueue(28))
    } yield () // -6

    assertResult((27, Queue(18, 28))) {
      (for {
        result1f    <- program1.commit.start
        result2f    <- program2.commit.start
        _           <- result2f.joinWithNever
        _           <- result1f.joinWithNever
        result      <- txnVar.get.commit
        resultQueue <- txnVarQueue.get.commit
      } yield (result, resultQueue)).unsafeRunSync()
    }
  }

}
