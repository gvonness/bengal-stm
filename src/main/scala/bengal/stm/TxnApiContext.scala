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
package bengal.stm

import cats.effect.kernel.Concurrent
import cats.free.Free

import scala.annotation.nowarn

private[stm] trait TxnApiContext[F[_]] {
  this: TxnAdtContext[F] with TxnStateEntityContext[F] =>

  private def liftSuccess[V](txnAdt: TxnAdt[V]): Txn[V] =
    Free.liftF[TxnOrErr, V](Right(txnAdt))

  private def liftFailure(txnErr: TxnErratum): Txn[Unit] =
    Free.liftF[TxnOrErr, Unit](Left(txnErr))

  val unit: Txn[Unit] =
    liftSuccess(TxnUnit)

  def pure[V](value: => V): Txn[V] =
    liftSuccess(TxnPure(() => value))

  def waitFor(predicate: => Boolean): Txn[Unit] =
    if (predicate) unit
    else
      liftFailure(TxnRetry)

  def abort(ex: Throwable): Txn[Unit] =
    liftFailure(TxnError(ex))

  private[stm] def handleErrorWithInternal[V](fa: => Txn[V])(
      f: Throwable => Txn[V]
  ): Txn[V] =
    liftSuccess(TxnHandleError(() => fa, f))

  private[stm] def getTxnVar[V](txnVar: => TxnVar[V]): Txn[V] =
    liftSuccess(TxnGetVar(() => txnVar))

  private[stm] def setTxnVar[V](
      newValue: => V,
      txnVar: => TxnVar[V]
  ): Txn[Unit] =
    liftSuccess(TxnSetVar(() => newValue, () => txnVar))

  private[stm] def modifyTxnVar[V](f: V => V, txnVar: => TxnVar[V]): Txn[Unit] =
    for {
      value <- getTxnVar(txnVar)
      _     <- setTxnVar(f(value), txnVar)
    } yield ()

  private[stm] def getTxnVarMap[K, V](
      txnVarMap: => TxnVarMap[K, V]
  ): Txn[Map[K, V]] =
    liftSuccess(TxnGetVarMap(() => txnVarMap))

  private[stm] def setTxnVarMap[K, V](
      newValueMap: => Map[K, V],
      txnVarMap: => TxnVarMap[K, V]
  ): Txn[Unit] =
    liftSuccess(TxnSetVarMap(() => newValueMap, () => txnVarMap))

  private[stm] def modifyTxnVarMap[K, V](
      f: Map[K, V] => Map[K, V],
      txnVarMap: => TxnVarMap[K, V]
  ): Txn[Unit] =
    for {
      value <- getTxnVarMap(txnVarMap)
      _     <- setTxnVarMap(f(value), txnVarMap)
    } yield ()

  @nowarn
  private[stm] def getTxnVarMapValue[K, V](
      key: => K,
      txnVarMap: => TxnVarMap[K, V]
  )(implicit F: Concurrent[F]): Txn[Option[V]] =
    liftSuccess(TxnGetVarMapValue(() => key, () => txnVarMap))

  private[stm] def setTxnVarMapValue[K, V](
      key: => K,
      newValue: => V,
      txnVarMap: => TxnVarMap[K, V]
  ): Txn[Unit] =
    liftSuccess(TxnSetVarMapValue(() => key, () => newValue, () => txnVarMap))

  private[stm] def modifyTxnVarMapValue[K, V](
      key: => K,
      f: V => V,
      txnVarMap: => TxnVarMap[K, V]
  ): Txn[Unit] =
    liftSuccess(TxnModifyVarMapValue(() => key, f, () => txnVarMap))

  private[stm] def removeTxnVarMapValue[K, V](
      key: => K,
      txnVarMap: => TxnVarMap[K, V]
  ): Txn[Unit] =
    liftSuccess(TxnDeleteVarMapValue(() => key, () => txnVarMap))
}
