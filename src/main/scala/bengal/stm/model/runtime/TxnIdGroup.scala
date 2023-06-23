package ai.entrolution
package bengal.stm.model.runtime

import scala.collection.concurrent.TrieMap


private[stm] case class TxnIdGroup(updateId: Option[TxnId], readIds: TrieMap[TxnId, Unit]) {
  private[stm] def addReadId(id: TxnId): Unit =
    readIds.addOne(id -> ())
}

object TxnIdGroup {
  private[stm] def apply(id: TxnId) =
    TxnIdGroup(updateId = Some(id), readIds = TrieMap.empty)

  private[stm] def empty =
    TxnIdGroup(updateId = None, readIds = TrieMap.empty)
}