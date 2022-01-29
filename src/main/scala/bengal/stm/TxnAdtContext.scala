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
package bengal.stm

import cats.free.Free

private[stm] trait TxnAdtContext[F[_]] { this: TxnStateEntityContext[F] =>

  type Txn[V]                   = Free[TxnOrErr, V]
  private[stm] type TxnOrErr[V] = Either[TxnErratum, TxnAdt[V]]

  private[stm] sealed trait TxnErratum

  private[stm] case object NoErratum              extends TxnErratum
  private[stm] case object TxnRetry               extends TxnErratum
  private[stm] case class TxnError(ex: Throwable) extends TxnErratum

  private[stm] sealed trait TxnAdt[V]

  private[stm] case object TxnUnit extends TxnAdt[Unit]

  private[stm] case class TxnPure[V](value: V) extends TxnAdt[V]

  private[stm] case class TxnGetVar[V](txnVar: TxnVar[V]) extends TxnAdt[V]

  private[stm] case class TxnSetVar[V](newValue: V, txnVar: TxnVar[V])
      extends TxnAdt[Unit]

  private[stm] case class TxnGetVarMap[K, V](txnVarMap: TxnVarMap[K, V])
      extends TxnAdt[Map[K, V]]

  private[stm] case class TxnGetVarMapValue[K, V](
      key: K,
      txnVarMap: TxnVarMap[K, V]
  ) extends TxnAdt[Option[V]]

  private[stm] case class TxnSetVarMap[K, V](
      newMap: Map[K, V],
      txnVarMap: TxnVarMap[K, V]
  ) extends TxnAdt[Unit]

  private[stm] case class TxnSetVarMapValue[K, V](
      key: K,
      newValue: V,
      txnVarMap: TxnVarMap[K, V]
  ) extends TxnAdt[Unit]

  private[stm] case class TxnModifyVarMapValue[K, V](
      key: K,
      f: V => V,
      txnVarMap: TxnVarMap[K, V]
  ) extends TxnAdt[Unit]

  private[stm] case class TxnDeleteVarMapValue[K, V](
      key: K,
      txnVarMap: TxnVarMap[K, V]
  ) extends TxnAdt[Unit]

  private[stm] case class TxnHandleError[V](
      fa: Txn[V],
      f: Throwable => Txn[V]
  ) extends TxnAdt[V]
}
