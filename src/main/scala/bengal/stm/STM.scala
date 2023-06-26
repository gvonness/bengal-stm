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
package bengal.stm

import bengal.stm.api.internal.TxnApiContext
import bengal.stm.model._
import bengal.stm.model.runtime._
import bengal.stm.runtime.{TxnCompilerContext, TxnLogContext, TxnRuntimeContext}

import cats.effect.Ref
import cats.effect.kernel.Async
import cats.effect.std.Semaphore
import cats.implicits._

import scala.concurrent.duration.{FiniteDuration, NANOSECONDS}

abstract class STM[F[_]: Async]
    extends AsyncImplicits[F]
    with TxnRuntimeContext[F]
    with TxnCompilerContext[F]
    with TxnLogContext[F]
    with TxnApiContext[F]
    with TxnAdtContext[F] {

  def allocateTxnVar[V](value: V): F[TxnVar[F, V]]
  def allocateTxnVarMap[K, V](valueMap: Map[K, V]): F[TxnVarMap[F, K, V]]
  private[stm] def commitTxn[V](txn: Txn[V]): F[V]

  implicit class TxnVarOps[V](txnVar: TxnVar[F, V]) {

    def get: Txn[V] =
      getTxnVar(txnVar)

    def set(newValue: => V): Txn[Unit] =
      setTxnVar(newValue, txnVar)

    def modify(f: V => V): Txn[Unit] =
      modifyTxnVar(f, txnVar)
  }

  implicit class TxnVarMapOps[K, V](txnVarMap: TxnVarMap[F, K, V]) {

    def get: Txn[Map[K, V]] =
      getTxnVarMap(txnVarMap)

    def set(newValueMap: => Map[K, V]): Txn[Unit] =
      setTxnVarMap(newValueMap, txnVarMap)

    def modify(f: Map[K, V] => Map[K, V]): Txn[Unit] =
      modifyTxnVarMap(f, txnVarMap)

    def get(key: => K): Txn[Option[V]] =
      getTxnVarMapValue(key, txnVarMap)

    def set(key: => K, newValue: => V): Txn[Unit] =
      setTxnVarMapValue(key, newValue, txnVarMap)

    def modify(key: => K, f: V => V): Txn[Unit] =
      modifyTxnVarMapValue(key, f, txnVarMap)

    def remove(key: => K): Txn[Unit] =
      removeTxnVarMapValue(key, txnVarMap)
  }

  implicit class TxnOps[V](txn: => Txn[V]) {

    def commit: F[V] =
      commitTxn(txn)

    def handleErrorWith(f: Throwable => Txn[V]): Txn[V] =
      handleErrorWithInternal(txn)(f)
  }
}

object STM {

  def apply[F[_]](implicit stm: STM[F]): STM[F] =
    stm

  def runtime[F[_]: Async]: F[STM[F]] =
    runtime(FiniteDuration(Long.MaxValue, NANOSECONDS))

  def runtime[F[_]: Async](
      retryMaxWait: FiniteDuration
  ): F[STM[F]] =
    for {
      idGenVar            <- Ref.of[F, Long](0)
      idGenTxn            <- Ref.of[F, Long](0)
      graphBuilderSemaphore    <- Semaphore[F](1)
      stm <- Async[F].delay {
               new STM[F] {
                 override val txnVarIdGen: Ref[F, TxnVarId] = idGenVar
                 override val txnIdGen: Ref[F, TxnId]       = idGenTxn

                 val txnRuntime: TxnRuntime = new TxnRuntime {
                   override val scheduler: TxnScheduler =
                     TxnScheduler(
                       graphBuilderSemaphore = graphBuilderSemaphore,
                       retryWaitMaxDuration = retryMaxWait
                     )
                 }

                 override def allocateTxnVar[V](value: V): F[TxnVar[F, V]] =
                   TxnVar.of(value)(this, this.asyncF)

                 override def allocateTxnVarMap[K, V](
                     valueMap: Map[K, V]
                 ): F[TxnVarMap[F, K, V]] =
                   TxnVarMap.of(valueMap)(this, this.asyncF)

                 override private[stm] def commitTxn[V](txn: Txn[V]): F[V] =
                   txnRuntime.commit(txn)
               }
             }
    } yield stm
}
