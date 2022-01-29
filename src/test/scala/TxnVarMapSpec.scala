/*
 * Copyright 2020-2021 Entrolution
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

class TxnVarMapSpec extends AnyFlatSpec {
  "TxnVarMap.get" should "return the value of a transactional map" in new StmRuntimeFixture {
    import stm._

    val baseMap = Map("foo" -> 42, "bar" -> 27, "baz" -> 18)

    val tVarMap: TxnVarMap[String, Int] = TxnVarMap.of(baseMap).unsafeRunSync()

    assertResult(baseMap) {
      tVarMap.get.commit.unsafeRunSync()
    }
  }

  "TxnVarMap.set" should "should update the underpinning map" in new StmRuntimeFixture {
    import stm._

    val baseMap = Map("foo" -> 42, "bar" -> 27, "baz" -> 18)
    val newMap  = Map("foo" -> -10, "foobaz" -> 31)

    val tVarMap: TxnVarMap[String, Int] = TxnVarMap.of(baseMap).unsafeRunSync()

    tVarMap.set(newMap).commit.unsafeRunSync()

    assertResult(newMap) {
      tVarMap.get.commit.unsafeRunSync()
    }
  }

  "TxnVarMap.get(key)" should "return the value of transactional variable" in new StmRuntimeFixture {
    import stm._

    val baseMap = Map("foo" -> 42, "bar" -> 27, "baz" -> 18)

    val tVarMap: TxnVarMap[String, Int] = TxnVarMap.of(baseMap).unsafeRunSync()

    assertResult(Some(42)) {
      tVarMap.get("foo").commit.unsafeRunSync()
    }
  }

  "TxnVarMap.get(key)" should "throw an error if key isn't present" in new StmRuntimeFixture {
    import stm._

    val baseMap = Map("foo" -> 42, "bar" -> 27, "baz" -> 18)

    val tVarMap: TxnVarMap[String, Int] = TxnVarMap.of(baseMap).unsafeRunSync()

    assertThrows[RuntimeException] {
      tVarMap.get("foobar").commit.unsafeRunSync()
    }
  }

  "TxnVarMap.get(key)" should "return None if the key is deleted in the current transaction" in new StmRuntimeFixture {
    import stm._

    val baseMap = Map("foo" -> 42, "bar" -> 27, "baz" -> 18)

    val tVarMap: TxnVarMap[String, Int] = TxnVarMap.of(baseMap).unsafeRunSync()

    assertResult(None) {
      (for {
        _      <- tVarMap.remove("foo")
        result <- tVarMap.get("foo")
      } yield result).commit.unsafeRunSync()
    }
  }

  "TxnVarMap.set(key)" should "update values for existing keys" in new StmRuntimeFixture {
    import stm._

    val baseMap = Map("foo" -> 42, "bar" -> 27, "baz" -> 18)

    val tVarMap: TxnVarMap[String, Int] = TxnVarMap.of(baseMap).unsafeRunSync()

    tVarMap.set("foo", 2).commit.unsafeRunSync()

    assertResult(Some(2)) {
      tVarMap.get("foo").commit.unsafeRunSync()
    }
  }

  "TxnVarMap.set(key)" should "creates new entry for non-existent key" in new StmRuntimeFixture {
    import stm._

    val baseMap = Map("foo" -> 42, "bar" -> 27, "baz" -> 18)

    val tVarMap: TxnVarMap[String, Int] = TxnVarMap.of(baseMap).unsafeRunSync()

    tVarMap.set("foobaz", 2).commit.unsafeRunSync()

    assertResult(Some(2)) {
      tVarMap.get("foobaz").commit.unsafeRunSync()
    }
  }

  "TxnVarMap.modify(key)" should "modify value for pre-existing entry" in new StmRuntimeFixture {
    import stm._

    val baseMap = Map("foo" -> 42, "bar" -> 27, "baz" -> 18)

    val tVarMap: TxnVarMap[String, Int] = TxnVarMap.of(baseMap).unsafeRunSync()

    tVarMap.modify("baz", _ - 12).commit.unsafeRunSync()

    assertResult(Some(6)) {
      tVarMap.get("baz").commit.unsafeRunSync()
    }
  }

  "TxnVarMap.modify" should "throw an error if key isn't present" in new StmRuntimeFixture {
    import stm._

    val baseMap = Map("foo" -> 42, "bar" -> 27, "baz" -> 18)

    val tVarMap: TxnVarMap[String, Int] = TxnVarMap.of(baseMap).unsafeRunSync()

    assertThrows[RuntimeException] {
      tVarMap.modify("foobar", _ + 2).commit.unsafeRunSync()
    }
  }

  "TxnVarMap.modify" should "modify value for key created in current transaction" in new StmRuntimeFixture {
    import stm._

    val baseMap = Map("foo" -> 42, "bar" -> 27, "baz" -> 18)

    val tVarMap: TxnVarMap[String, Int] = TxnVarMap.of(baseMap).unsafeRunSync()

    assertResult(Some(25)) {
      (for {
        _      <- tVarMap.set("foobaz", 3)
        _      <- tVarMap.modify("foobaz", _ + 22)
        result <- tVarMap.get("foobaz")
      } yield result).commit.unsafeRunSync()
    }
  }

  "TxnVarMap.remove(key)" should "remove value for pre-existing entry" in new StmRuntimeFixture {
    import stm._

    val baseMap = Map("foo" -> 42, "bar" -> 27, "baz" -> 18)

    val tVarMap: TxnVarMap[String, Int] = TxnVarMap.of(baseMap).unsafeRunSync()

    tVarMap.remove("baz").commit.unsafeRunSync()

    assertResult(Map("foo" -> 42, "bar" -> 27)) {
      tVarMap.get.commit.unsafeRunSync()
    }
  }

  "TxnVarMap.remove(key)" should "throw an error if key doesn't exist" in new StmRuntimeFixture {
    import stm._

    val baseMap = Map("foo" -> 42, "bar" -> 27, "baz" -> 18)

    val tVarMap: TxnVarMap[String, Int] = TxnVarMap.of(baseMap).unsafeRunSync()

    assertThrows[RuntimeException] {
      tVarMap.remove("foobar").commit.unsafeRunSync()
    }
  }

  "TxnVarMap.remove(key)" should "remove value of entry created in current transaction" in new StmRuntimeFixture {
    import stm._

    val baseMap = Map("foo" -> 42, "bar" -> 27, "baz" -> 18)

    val tVarMap: TxnVarMap[String, Int] = TxnVarMap.of(baseMap).unsafeRunSync()

    assertResult(None) {
      (for {
        _      <- tVarMap.set("foobar", 22)
        _      <- tVarMap.remove("foobar")
        result <- tVarMap.get("foobar")
      } yield result).commit.unsafeRunSync()
    }
  }
}
