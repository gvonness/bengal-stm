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
package bengal.stm.model

import bengal.stm.STM
import bengal.stm.model.runtime._

import cats.effect.implicits._
import cats.effect.kernel.Async
import cats.effect.std.Semaphore
import cats.effect.{Deferred, Ref}
import cats.syntax.all._

import java.util.UUID
import scala.collection.mutable.{Map => MutableMap}

case class TxnVarMap[F[_]: STM: Async, K, V](
    private[stm] val id: TxnVarId,
    protected val value: Ref[F, VarIndex[F, K, V]],
    private[stm] val commitLock: Semaphore[F],
    private val internalStructureLock: Semaphore[F],
    private val internalSignalLock: Semaphore[F],
    private[stm] val txnRetrySignals: TxnSignals[F]
) extends TxnStateEntity[F, VarIndex[F, K, V]] {

  private def withLock[A](semaphore: Semaphore[F])(
      fa: F[A]
  ): F[A] =
    semaphore.permit.use(_ => fa)

  private[stm] lazy val get: F[Map[K, V]] =
    for {
      txnVarMap <- value.get
      valueMap <- txnVarMap.toList.parTraverse { kv =>
                    kv._2.get.map(v => kv._1 -> v)
                  }
    } yield valueMap.toMap

  private[stm] def getTxnVar(key: K): F[Option[TxnVar[F, V]]] =
    for {
      txnVarMap <- value.get
    } yield txnVarMap.get(key)

  private[stm] def get(key: K): F[Option[V]] =
    for {
      oTxnVar <- getTxnVar(key)
      result <- oTxnVar match {
                  case Some(txnVar) =>
                    txnVar.get.map(v => Some(v))
                  case _ =>
                    Async[F].pure(None)
                }
    } yield result

  private[stm] def getId(key: K): F[Option[TxnVarId]] =
    getTxnVar(key).map(_.map(_.id))

  private[stm] def getRuntimeExistentialId(key: K): TxnVarRuntimeId =
    UUID.nameUUIDFromBytes((id, key).toString.getBytes).hashCode()

  private[stm] def getRuntimeActualisedId(
      key: K
  ): F[Option[TxnVarRuntimeId]] =
    getTxnVar(key).map(_.map(_.runtimeId))

  private[stm] def getRuntimeId(
      key: K
  ): F[List[TxnVarRuntimeId]] =
    getRuntimeActualisedId(key).map(
      List(_, Some(getRuntimeExistentialId(key))).flatten
    )

  // Get transactional IDs for any keys already existing
  // in the map
  private[stm] def getIdsForKeys(
      keySet: Set[K]
  ): F[Set[TxnVarId]] =
    for {
      ids <- keySet.toList.parTraverse(getId)
    } yield ids.flatten.toSet

  // Only called when key is known to not exist
  private def add(newKey: K, newValue: V): F[Unit] =
    for {
      newTxnVar <- TxnVar.of(newValue)
      _ <- withLock(internalStructureLock)(
             value.update(_ += (newKey -> newTxnVar))
           )
    } yield ()

  private[stm] def addOrUpdate(key: K, newValue: V): F[Unit] =
    for {
      txnVarMap <- value.get
      _ <- txnVarMap.get(key) match {
             case Some(tVar) =>
               withLock(internalStructureLock)(tVar.set(newValue))
             case None =>
               add(key, newValue)
           }
    } yield ()

  private[stm] def delete(key: K): F[Unit] =
    for {
      txnVarMap <- value.get
      _ <- txnVarMap.get(key) match {
             case Some(_) =>
               withLock(internalStructureLock)(value.update(_ -= key))
             case None =>
               Async[F].unit
           }
    } yield ()

  override private[stm] def registerRetry(
      signal: Deferred[F, Unit]
  ): F[Unit] =
    withLock(internalSignalLock)(txnRetrySignals.update(_ + signal))
}

object TxnVarMap {

  def of[F[_]: STM: Async, K, V](valueMap: Map[K, V]): F[TxnVarMap[F, K, V]] =
    for {
      id <- STM[F].txnVarIdGen.updateAndGet(_ + 1)
      values <- valueMap.toList.traverse { kv =>
                  TxnVar.of(kv._2).map(txv => kv._1 -> txv)
                }
      valuesRef             <- Async[F].ref(MutableMap(values: _*))
      lock                  <- Semaphore[F](1)
      internalStructureLock <- Semaphore[F](1)
      internalSignalLock    <- Semaphore[F](1)
      signals               <- Async[F].ref(Set[Deferred[F, Unit]]())
    } yield TxnVarMap(id,
                      valuesRef,
                      lock,
                      internalStructureLock,
                      internalSignalLock,
                      signals
    )
}
