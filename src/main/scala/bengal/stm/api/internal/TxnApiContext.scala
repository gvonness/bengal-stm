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
package bengal.stm.api.internal

import bengal.stm.model.TxnErratum._
import bengal.stm.model._
import bengal.stm.runtime.TxnRuntimeContext

import cats.effect.kernel.Async
import cats.free.Free

import scala.util.{ Failure, Success, Try }

private[stm] trait TxnApiContext[F[_]] {
  this: AsyncImplicits[F] with TxnRuntimeContext[F] with TxnAdtContext[F] =>

  private def liftSuccess[V](txnAdt: TxnAdt[V]): Txn[V] =
    Free.liftF[TxnOrErr, V](Right(txnAdt))

  private def liftFailure(txnErr: TxnErratum): Txn[Unit] =
    Free.liftF[TxnOrErr, Unit](Left(txnErr))

  val unit: Txn[Unit] =
    liftSuccess(TxnUnit)

  def fromF[V](spec: F[V]): Txn[V] =
    liftSuccess(TxnDelay(spec))

  def delay[V](thunk: => V): Txn[V] =
    liftSuccess(TxnDelay(Async[F].delay(thunk)))

  def pure[V](value: V): Txn[V] =
    liftSuccess(TxnPure(value))

  def waitFor(predicate: => Boolean): Txn[Unit] =
    Try(predicate) match {
      case Success(true) =>
        unit
      case Success(_) =>
        liftFailure(TxnRetry)
      case Failure(exception) =>
        abort(exception)
    }

  def abort(ex: Throwable): Txn[Unit] =
    liftFailure(TxnError(ex))

  private[stm] def handleErrorWithInternal[V](fa: => Txn[V])(
    f: Throwable => Txn[V]
  ): Txn[V] =
    liftSuccess(TxnHandleError(Async[F].delay(fa), ex => Async[F].delay(f(ex))))

  private[stm] def handleErrorWithInternalF[V](fa: => Txn[V])(
    f: Throwable => F[Txn[V]]
  ): Txn[V] =
    liftSuccess(TxnHandleError(Async[F].delay(fa), f))

  private[stm] def getTxnVar[V](txnVar: TxnVar[F, V]): Txn[V] =
    liftSuccess(TxnGetVar(txnVar))

  private[stm] def setTxnVar[V](
    newValue: => V,
    txnVar: TxnVar[F, V]
  ): Txn[Unit] =
    liftSuccess(TxnSetVar(Async[F].delay(newValue), txnVar))

  private[stm] def setTxnVarF[V](
    newValue: F[V],
    txnVar: TxnVar[F, V]
  ): Txn[Unit] =
    liftSuccess(TxnSetVar(newValue, txnVar))

  private[stm] def modifyTxnVar[V](f: V => V, txnVar: TxnVar[F, V]): Txn[Unit] =
    for {
      value <- getTxnVar(txnVar)
      _     <- setTxnVar(f(value), txnVar)
    } yield ()

  private[stm] def modifyTxnVarF[V](
    f: V => F[V],
    txnVar: TxnVar[F, V]
  ): Txn[Unit] =
    for {
      value <- getTxnVar(txnVar)
      _     <- setTxnVarF(f(value), txnVar)
    } yield ()

  private[stm] def getTxnVarMap[K, V](
    txnVarMap: TxnVarMap[F, K, V]
  ): Txn[Map[K, V]] =
    liftSuccess(TxnGetVarMap(txnVarMap))

  private[stm] def setTxnVarMap[K, V](
    newValueMap: => Map[K, V],
    txnVarMap: TxnVarMap[F, K, V]
  ): Txn[Unit] =
    liftSuccess(TxnSetVarMap(Async[F].delay(newValueMap), txnVarMap))

  private[stm] def setTxnVarMapF[K, V](
    newValueMap: F[Map[K, V]],
    txnVarMap: TxnVarMap[F, K, V]
  ): Txn[Unit] =
    liftSuccess(TxnSetVarMap(newValueMap, txnVarMap))

  private[stm] def modifyTxnVarMap[K, V](
    f: Map[K, V] => Map[K, V],
    txnVarMap: TxnVarMap[F, K, V]
  ): Txn[Unit] =
    for {
      value <- getTxnVarMap(txnVarMap)
      _     <- setTxnVarMap(f(value), txnVarMap)
    } yield ()

  private[stm] def modifyTxnVarMapF[K, V](
    f: Map[K, V] => F[Map[K, V]],
    txnVarMap: TxnVarMap[F, K, V]
  ): Txn[Unit] =
    for {
      value <- getTxnVarMap(txnVarMap)
      _     <- setTxnVarMapF(f(value), txnVarMap)
    } yield ()

  private[stm] def getTxnVarMapValue[K, V](
    key: => K,
    txnVarMap: TxnVarMap[F, K, V]
  ): Txn[Option[V]] =
    liftSuccess(TxnGetVarMapValue(Async[F].delay(key), txnVarMap))

  private[stm] def setTxnVarMapValue[K, V](
    key: => K,
    newValue: => V,
    txnVarMap: TxnVarMap[F, K, V]
  ): Txn[Unit] =
    liftSuccess(
      TxnSetVarMapValue(Async[F].delay(key), Async[F].delay(newValue), txnVarMap)
    )

  private[stm] def setTxnVarMapValueF[K, V](
    key: => K,
    newValue: F[V],
    txnVarMap: TxnVarMap[F, K, V]
  ): Txn[Unit] =
    liftSuccess(
      TxnSetVarMapValue(Async[F].delay(key), newValue, txnVarMap)
    )

  private[stm] def modifyTxnVarMapValue[K, V](
    key: => K,
    f: V => V,
    txnVarMap: TxnVarMap[F, K, V]
  ): Txn[Unit] =
    liftSuccess(
      TxnModifyVarMapValue(Async[F].delay(key), (v: V) => Async[F].delay(f(v)), txnVarMap)
    )

  private[stm] def modifyTxnVarMapValueF[K, V](
    key: => K,
    f: V => F[V],
    txnVarMap: TxnVarMap[F, K, V]
  ): Txn[Unit] =
    liftSuccess(
      TxnModifyVarMapValue(Async[F].delay(key), f, txnVarMap)
    )

  private[stm] def removeTxnVarMapValue[K, V](
    key: => K,
    txnVarMap: TxnVarMap[F, K, V]
  ): Txn[Unit] =
    liftSuccess(TxnDeleteVarMapValue(Async[F].delay(key), txnVarMap))
}
