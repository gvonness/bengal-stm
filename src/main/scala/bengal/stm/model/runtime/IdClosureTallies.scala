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
package bengal.stm.model.runtime

import scala.collection.concurrent.{TrieMap, Map => ConcurrentMap}

private[stm] case class IdClosureTallies(
    private val readIdTallies: ConcurrentMap[TxnVarRuntimeId, Int],
    private val updatedIdTallies: ConcurrentMap[TxnVarRuntimeId, Int]
) {

  private def addReadId(id: TxnVarRuntimeId): Unit =
    readIdTallies += (id -> (readIdTallies.getOrElse(id, 0) + 1))

  private def removeReadId(id: TxnVarRuntimeId): Unit = {
    val newValue: Int = readIdTallies.getOrElse(id, 0) - 1
    if (newValue < 1) {
      readIdTallies -= id
    } else {
      readIdTallies += (id -> newValue)
    }
  }

  private def addUpdateId(id: TxnVarRuntimeId): Unit =
    updatedIdTallies += (id -> (updatedIdTallies.getOrElse(id, 0) + 1))

  private def removeUpdateId(id: TxnVarRuntimeId): Unit = {
    val newValue: Int = updatedIdTallies.getOrElse(id, 0) - 1
    if (newValue < 1) {
      updatedIdTallies -= id
    } else {
      updatedIdTallies += (id -> newValue)
    }
  }

  private def addReadIds(ids: Set[TxnVarRuntimeId]): Unit =
    ids.foreach(addReadId)

  private def removeReadIds(ids: Set[TxnVarRuntimeId]): Unit =
    ids.foreach(removeReadId)

  private def addUpdateIds(ids: Set[TxnVarRuntimeId]): Unit =
    ids.foreach(addUpdateId)

  private def removeUpdateIds(ids: Set[TxnVarRuntimeId]): Unit =
    ids.foreach(removeUpdateId)

  private[stm] def addIdClosure(idClosure: IdClosure): Unit = {
    addReadIds(idClosure.readIds)
    addUpdateIds(idClosure.updatedIds)
  }

  private[stm] def removeIdClosure(idClosure: IdClosure): Unit = {
    removeReadIds(idClosure.readIds)
    removeUpdateIds(idClosure.updatedIds)
  }

  private[stm] lazy val getIdClosure: IdClosure =
    IdClosure(
      readIds = readIdTallies.keySet.toSet,
      updatedIds = updatedIdTallies.keySet.toSet
    )
}

private[stm] object IdClosureTallies {

  private[stm] val empty: IdClosureTallies =
    IdClosureTallies(TrieMap.empty, TrieMap.empty)
}
