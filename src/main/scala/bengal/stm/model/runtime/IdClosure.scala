/*
 * Copyright 2020-2023 Greg von Nessi
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

private[stm] case class IdClosure(
    readIds: Set[TxnVarRuntimeId],
    updatedIds: Set[TxnVarRuntimeId]
) {

  private[stm] lazy val combinedIds: Set[TxnVarRuntimeId] =
    readIds ++ updatedIds

  private[stm] def addReadId(id: TxnVarRuntimeId): IdClosure =
    this.copy(readIds = readIds + id)

  private[stm] def addWriteId(id: TxnVarRuntimeId): IdClosure =
    this.copy(updatedIds = updatedIds + id)

  private[stm] def mergeWith(idScope: IdClosure): IdClosure =
    this.copy(readIds = readIds ++ idScope.readIds,
              updatedIds = updatedIds ++ idScope.updatedIds
    )

  private[stm] def isCompatibleWith(idScope: IdClosure): Boolean =
    combinedIds.intersect(idScope.updatedIds).isEmpty && idScope.combinedIds
      .intersect(updatedIds)
      .isEmpty
}

private[stm] object IdClosure {
  private[stm] val empty: IdClosure = IdClosure(Set(), Set())
}
