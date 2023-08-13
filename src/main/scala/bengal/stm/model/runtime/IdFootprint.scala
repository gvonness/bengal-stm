/*
 * Copyright 2023 Greg von Nessi
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

private[stm] case class IdFootprint(
  readIds: Set[TxnVarRuntimeId],
  updatedIds: Set[TxnVarRuntimeId],
  isValidated: Boolean = false
) {

  private[stm] lazy val getValidated =
    if (isValidated) {
      this
    } else {
      this.copy(
        readIds     = (readIds -- updatedIds).filter(id => id.parent.forall(pid => !updateRawIds.contains(pid.value))),
        isValidated = true
      )
    }

  private[stm] lazy val combinedIds: Set[TxnVarRuntimeId] =
    readIds ++ updatedIds

  private[stm] lazy val combinedRawIds: Set[Int] = combinedIds.map(_.value)

  private[stm] lazy val updateRawIds: Set[Int] = updatedIds.map(_.value)

  private[stm] def addReadId(id: TxnVarRuntimeId): IdFootprint =
    this.copy(readIds = readIds + id)

  private[stm] def addWriteId(id: TxnVarRuntimeId): IdFootprint =
    this.copy(updatedIds = updatedIds + id)

  private[stm] def mergeWith(idScope: IdFootprint): IdFootprint =
    this.copy(readIds = readIds ++ idScope.readIds, updatedIds = updatedIds ++ idScope.updatedIds)

  private def asymmetricCompatibleWith(input: IdFootprint): Boolean =
    combinedRawIds.intersect(input.updateRawIds).isEmpty && !combinedIds.exists(
      _.parent.exists(p => input.updateRawIds.contains(p.value))
    )

  private[stm] def isCompatibleWith(input: IdFootprint): Boolean =
    asymmetricCompatibleWith(input) && input.asymmetricCompatibleWith(this)
}

private[stm] object IdFootprint {
  private[stm] val empty: IdFootprint = IdFootprint(Set(), Set())
}
