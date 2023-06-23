package ai.entrolution
package bengal.stm.model.runtime

private[stm] case class TxnVarRuntimeId(
    value: Int,
    parent: Option[TxnVarRuntimeId] = None
) {

  private[stm] def addParent(parent: TxnVarRuntimeId): TxnVarRuntimeId =
    this.copy(parent = Some(parent))
}
