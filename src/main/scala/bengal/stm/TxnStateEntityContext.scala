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
package bengal.stm

import bengal.stm.TxnStateEntityContext._

import cats.effect.kernel.Concurrent
import cats.effect.std.Semaphore
import cats.effect.{Deferred, Ref}
import cats.implicits._
import cats.effect.implicits._

import java.util.UUID
import scala.annotation.nowarn
import scala.collection.mutable.{Map => MutableMap}

private[stm] trait TxnStateEntityContext[F[_]] {

  protected val txnVarIdGen: Ref[F, TxnVarId]

  private[stm] type VarIndex[K, V] = MutableMap[K, TxnVar[V]]
  private[stm] type TxnSignals     = Ref[F, Set[Deferred[F, Unit]]]

  private[stm] sealed trait TxnStateEntity[V] {
    private[stm] def id: TxnVarId

    // A unique identifier for key-values that may
    // not be present in the map. This is used to build
    // references in the runtime system.
    // Note: We run this through a deterministic UUID mapping
    // to mitigate the chance of increment-based IDs colliding
    // with bare hash codes
    private[stm] final val runtimeId: TxnVarRuntimeId =
      UUID.nameUUIDFromBytes(id.toString.getBytes).hashCode()

    protected def value: Ref[F, V]
    private[stm] def commitLock: Semaphore[F]
    private[stm] def txnRetrySignals: TxnSignals

    @nowarn
    private[stm] def registerRetry(signal: Deferred[F, Unit])(implicit
        F: Concurrent[F]
    ): F[Unit] =
      txnRetrySignals.update(_ + signal)
  }

  case class TxnVar[T](
      private[stm] val id: TxnVarId,
      protected val value: Ref[F, T],
      private[stm] val commitLock: Semaphore[F],
      private[stm] val txnRetrySignals: TxnSignals
  ) extends TxnStateEntity[T] {

    private def completeRetrySignals(implicit F: Concurrent[F]): F[Unit] =
      for {
        signals <- txnRetrySignals.getAndSet(Set())
        _       <- signals.toList.parTraverse(_.complete(()))
      } yield ()

    private[stm] def get: F[T] =
      value.get

    private[stm] def set(
        newValue: T
    )(implicit F: Concurrent[F]): F[Unit] =
      for {
        _ <- value.set(newValue)
        _ <- completeRetrySignals
      } yield ()
  }

  object TxnVar {

    def of[T](value: T)(implicit F: Concurrent[F]): F[TxnVar[T]] =
      for {
        id       <- txnVarIdGen.updateAndGet(_ + 1)
        valueRef <- F.ref(value)
        lock     <- Semaphore[F](1)
        signals  <- F.ref(Set[Deferred[F, Unit]]())
      } yield TxnVar(id, valueRef, lock, signals)
  }

  case class TxnVarMap[K, V](
      private[stm] val id: TxnVarId,
      protected val value: Ref[F, VarIndex[K, V]],
      private[stm] val commitLock: Semaphore[F],
      private val internalStructureLock: Semaphore[F],
      private val internalSignalLock: Semaphore[F],
      private[stm] val txnRetrySignals: TxnSignals
  ) extends TxnStateEntity[VarIndex[K, V]] {

    private def withLock[A](semaphore: Semaphore[F])(fa: F[A])(implicit
        F: Concurrent[F]
    ): F[A] =
      semaphore.permit.use(_ => fa)

    private[stm] def get(implicit F: Concurrent[F]): F[Map[K, V]] =
      for {
        txnVarMap <- value.get
        valueMap <- txnVarMap.toList.parTraverse { kv =>
                      kv._2.get.map(v => kv._1 -> v)
                    }
      } yield valueMap.toMap

    private[stm] def getTxnVar(
        key: K
    )(implicit F: Concurrent[F]): F[Option[TxnVar[V]]] =
      for {
        txnVarMap <- value.get
      } yield txnVarMap.get(key)

    private[stm] def get(key: K)(implicit F: Concurrent[F]): F[Option[V]] =
      for {
        oTxnVar <- getTxnVar(key)
        result <- oTxnVar match {
                    case Some(txnVar) =>
                      txnVar.get.map(v => Some(v))
                    case _ =>
                      F.pure(None)
                  }
      } yield result

    private[stm] def getId(key: K)(implicit
        F: Concurrent[F]
    ): F[Option[TxnVarId]] =
      getTxnVar(key).map(_.map(_.id))

    private[stm] def getRuntimeExistentialId(key: K): TxnVarRuntimeId =
      UUID.nameUUIDFromBytes((id, key).toString.getBytes).hashCode()

    private[stm] def getRuntimeActualisedId(key: K)(implicit
        F: Concurrent[F]
    ): F[Option[TxnVarRuntimeId]] =
      getTxnVar(key).map(_.map(_.runtimeId))

    private[stm] def getRuntimeId(key: K)(implicit
        F: Concurrent[F]
    ): F[List[TxnVarRuntimeId]] =
      getRuntimeActualisedId(key).map(
        List(_, Some(getRuntimeExistentialId(key))).flatten
      )

    // Get transactional IDs for any keys already existing
    // in the map
    private[stm] def getIdsForKeys(
        keySet: Set[K]
    )(implicit F: Concurrent[F]): F[Set[TxnVarId]] =
      for {
        ids <- keySet.toList.parTraverse(getId)
      } yield ids.flatten.toSet

    // Only called when key is known to not exist
    private def add(newKey: K, newValue: V)(implicit
        F: Concurrent[F]
    ): F[Unit] =
      for {
        newTxnVar <- TxnVar.of(newValue)
        _ <- withLock(internalStructureLock)(
               value.update(_ += (newKey -> newTxnVar))
             )
      } yield ()

    private[stm] def addOrUpdate(key: K, newValue: V)(implicit
        F: Concurrent[F]
    ): F[Unit] =
      for {
        txnVarMap <- value.get
        _ <- txnVarMap.get(key) match {
               case Some(tVar) =>
                 withLock(internalStructureLock)(tVar.set(newValue))
               case None =>
                 add(key, newValue)
             }
      } yield ()

    private[stm] def delete(key: K)(implicit F: Concurrent[F]): F[Unit] =
      for {
        txnVarMap <- value.get
        _ <- txnVarMap.get(key) match {
               case Some(_) =>
                 withLock(internalStructureLock)(value.update(_ -= key))
               case None =>
                 F.unit
             }
      } yield ()

    override private[stm] def registerRetry(
        signal: Deferred[F, Unit]
    )(implicit F: Concurrent[F]): F[Unit] =
      withLock(internalSignalLock)(txnRetrySignals.update(_ + signal))
  }

  object TxnVarMap {

    def of[K, V](
        valueMap: Map[K, V]
    )(implicit F: Concurrent[F]): F[TxnVarMap[K, V]] =
      for {
        id <- txnVarIdGen.updateAndGet(_ + 1)
        values <- valueMap.toList.traverse { kv =>
                    TxnVar.of(kv._2).map(txv => kv._1 -> txv)
                  }
        valuesRef             <- F.ref(MutableMap(values: _*))
        lock                  <- Semaphore[F](1)
        internalStructureLock <- Semaphore[F](1)
        internalSignalLock    <- Semaphore[F](1)
        signals               <- F.ref(Set[Deferred[F, Unit]]())
      } yield TxnVarMap(id,
                        valuesRef,
                        lock,
                        internalStructureLock,
                        internalSignalLock,
                        signals
      )
  }
}

private[stm] object TxnStateEntityContext {
  private[stm] type TxnVarId        = Long
  private[stm] type TxnVarRuntimeId = Int
  private[stm] type TxnId           = Long
}
