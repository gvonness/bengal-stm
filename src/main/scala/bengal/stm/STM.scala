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

import bengal.stm.TxnRuntimeContext.IdClosureTallies
import bengal.stm.TxnStateEntityContext.{TxnId, TxnVarId}

import cats.effect.Ref
import cats.effect.implicits._
import cats.effect.kernel.{Concurrent, Deferred, Temporal}
import cats.effect.std.Semaphore
import cats.implicits._

import scala.annotation.nowarn
import scala.concurrent.duration.{FiniteDuration, NANOSECONDS}

trait STM[F[_]]
    extends TxnApiContext[F]
    with TxnAdtContext[F]
    with TxnCompilerContext[F]
    with TxnStateEntityContext[F]
    with TxnLogContext[F]
    with TxnRuntimeContext[F] {

  protected val ConcurrentF: Concurrent[F]

  def allocateTxnVar[V](value: V): F[TxnVar[V]]
  def allocateTxnVarMap[K, V](valueMap: Map[K, V]): F[TxnVarMap[K, V]]
  protected def commitTxn[V](txn: Txn[V]): F[V]

  implicit class TxnVarOps[V](txnVar: TxnVar[V]) {

    def get: Txn[V] =
      getTxnVar(txnVar)

    def set(newValue: => V): Txn[Unit] =
      setTxnVar(newValue, txnVar)

    def modify(f: V => V): Txn[Unit] =
      modifyTxnVar(f, txnVar)
  }

  implicit class TxnVarMapOps[K, V](txnVarMap: TxnVarMap[K, V]) {

    def get: Txn[Map[K, V]] =
      getTxnVarMap(txnVarMap)

    def set(newValueMap: => Map[K, V]): Txn[Unit] =
      setTxnVarMap(newValueMap, txnVarMap)

    def modify(f: Map[K, V] => Map[K, V]): Txn[Unit] =
      modifyTxnVarMap(f, txnVarMap)

    def get(key: => K): Txn[Option[V]] =
      getTxnVarMapValue(key, txnVarMap)(ConcurrentF)

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

  @nowarn
  def runtime[F[_]: Concurrent: Temporal]: F[STM[F]] =
    runtime(FiniteDuration(Long.MaxValue, NANOSECONDS),
            Runtime.getRuntime.availableProcessors() * 2
    )

  @nowarn
  def runtime[F[_]: Concurrent: Temporal](
      retryMaxWait: FiniteDuration,
      maxWaitingToProcessInLoop: Int
  ): F[STM[F]] =
    for {
      idGenVar            <- Ref.of[F, Long](0)
      idGenTxn            <- Ref.of[F, Long](0)
      runningSemaphore    <- Semaphore[F](1)
      waitingSemaphore    <- Semaphore[F](1)
      closureSemaphore    <- Semaphore[F](1)
      schedulerTrigger    <- Deferred[F, Unit]
      schedulerTriggerRef <- Ref.of(schedulerTrigger)
      closureTalliesRef   <- Ref.of[F, IdClosureTallies](IdClosureTallies.empty)
      stm <- implicitly[Concurrent[F]].pure {
               new STM[F] {
                 override protected implicit val ConcurrentF: Concurrent[F] =
                   implicitly[Concurrent[F]]

                 override val txnVarIdGen: Ref[F, TxnVarId] = idGenVar
                 override val txnIdGen: Ref[F, TxnId]       = idGenTxn

                 val txnRuntime: TxnRuntime = new TxnRuntime {
                   override val scheduler: TxnScheduler =
                     TxnScheduler(
                       runningSemaphore,
                       waitingSemaphore,
                       closureSemaphore,
                       schedulerTriggerRef,
                       closureTalliesRef,
                       retryMaxWait,
                       maxWaitingToProcessInLoop
                     )
                 }

                 override def allocateTxnVar[V](value: V): F[TxnVar[V]] =
                   TxnVar.of(value)

                 override def allocateTxnVarMap[K, V](
                     valueMap: Map[K, V]
                 ): F[TxnVarMap[K, V]] = TxnVarMap.of(valueMap)

                 override protected def commitTxn[V](txn: Txn[V]): F[V] =
                   txnRuntime.commit(txn)
               }
             }
      _ <- stm.txnRuntime.scheduler.reprocessingRecursion.start
    } yield stm
}
