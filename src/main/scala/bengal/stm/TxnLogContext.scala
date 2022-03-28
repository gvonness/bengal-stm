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

import bengal.stm.TxnRuntimeContext.IdClosure
import bengal.stm.TxnStateEntityContext.TxnVarRuntimeId

import cats.effect.Deferred
import cats.effect.implicits._
import cats.effect.kernel.{Concurrent, Resource}
import cats.effect.std.Semaphore
import cats.implicits._

import scala.annotation.nowarn

private[stm] trait TxnLogContext[F[_]] { this: TxnStateEntityContext[F] =>

  private[stm] sealed trait TxnLogEntry[V] {
    private[stm] def get: V
    private[stm] def set(value: V): TxnLogEntry[V]
    private[stm] def commit(implicit F: Concurrent[F]): F[Unit]
    private[stm] def isDirty(implicit F: Concurrent[F]): F[Boolean]
    private[stm] def lock(implicit F: Concurrent[F]): F[Option[Semaphore[F]]]
    private[stm] def idClosure(implicit F: Concurrent[F]): F[IdClosure]

    private[stm] def getRegisterRetry(implicit
        F: Concurrent[F]
    ): F[Deferred[F, Unit] => F[Unit]]
  }

  // RO entry is not necessary for pure transactions.
  // However, to make the library interface more
  // predictable in the face of side-effects, RO
  // entries are created
  private[stm] case class TxnLogReadOnlyVarEntry[V](
      initial: V,
      txnVar: TxnVar[V]
  ) extends TxnLogEntry[V] {

    override private[stm] def get: V =
      initial

    override private[stm] def set(value: V): TxnLogEntry[V] =
      if (value != initial) {
        TxnLogUpdateVarEntry[V](
          initial = initial,
          current = value,
          txnVar = txnVar
        )
      } else {
        this
      }

    override private[stm] def commit(implicit F: Concurrent[F]): F[Unit] =
      F.unit

    override private[stm] def isDirty(implicit F: Concurrent[F]): F[Boolean] =
      F.pure(false)

    override private[stm] def lock(implicit
        F: Concurrent[F]
    ): F[Option[Semaphore[F]]] =
      F.pure(None)

    override private[stm] def getRegisterRetry(implicit
        F: Concurrent[F]
    ): F[Deferred[F, Unit] => F[Unit]] =
      F.pure(txnVar.registerRetry)

    override private[stm] def idClosure(implicit
        F: Concurrent[F]
    ): F[IdClosure] =
      F.pure {
        IdClosure(readIds = Set(txnVar.runtimeId), updatedIds = Set())
      }
  }

  private[stm] case class TxnLogUpdateVarEntry[V](
      initial: V,
      current: V,
      txnVar: TxnVar[V]
  ) extends TxnLogEntry[V] {

    override private[stm] def get: V =
      current

    override private[stm] def set(value: V): TxnLogEntry[V] =
      if (initial != value) {
        TxnLogUpdateVarEntry[V](
          initial = initial,
          current = value,
          txnVar = txnVar
        )
      } else {
        TxnLogReadOnlyVarEntry[V](
          initial = initial,
          txnVar = txnVar
        )
      }

    override private[stm] def commit(implicit F: Concurrent[F]): F[Unit] =
      txnVar.set(current)

    override private[stm] def isDirty(implicit F: Concurrent[F]): F[Boolean] =
      txnVar.get.map(_ != initial)

    override private[stm] def lock(implicit
        F: Concurrent[F]
    ): F[Option[Semaphore[F]]] =
      F.pure(Some(txnVar.commitLock))

    override private[stm] def getRegisterRetry(implicit
        F: Concurrent[F]
    ): F[Deferred[F, Unit] => F[Unit]] =
      F.pure(txnVar.registerRetry)

    override private[stm] def idClosure(implicit
        F: Concurrent[F]
    ): F[IdClosure] =
      F.pure {
        IdClosure(
          readIds = Set(txnVar.runtimeId),
          updatedIds = Set(txnVar.runtimeId)
        )
      }
  }

  // See above comment for RO entry
  private[stm] case class TxnLogReadOnlyVarMapStructureEntry[K, V](
      initial: Map[K, V],
      txnVarMap: TxnVarMap[K, V]
  ) extends TxnLogEntry[Map[K, V]] {

    override private[stm] def get: Map[K, V] =
      initial

    override private[stm] def set(value: Map[K, V]): TxnLogEntry[Map[K, V]] =
      if (initial != value) {
        TxnLogUpdateVarMapStructureEntry(
          initial = initial,
          current = value,
          txnVarMap = txnVarMap
        )
      } else {
        this
      }

    override private[stm] def commit(implicit F: Concurrent[F]): F[Unit] =
      F.unit

    override private[stm] def isDirty(implicit F: Concurrent[F]): F[Boolean] =
      F.pure(false)

    override private[stm] def lock(implicit
        F: Concurrent[F]
    ): F[Option[Semaphore[F]]] =
      F.pure(None)

    override private[stm] def getRegisterRetry(implicit
        F: Concurrent[F]
    ): F[Deferred[F, Unit] => F[Unit]] =
      F.pure(txnVarMap.registerRetry)

    override private[stm] def idClosure(implicit
        F: Concurrent[F]
    ): F[IdClosure] =
      F.pure {
        IdClosure(
          readIds = Set(txnVarMap.runtimeId),
          updatedIds = Set()
        )
      }
  }

  private[stm] case class TxnLogUpdateVarMapStructureEntry[K, V](
      initial: Map[K, V],
      current: Map[K, V],
      txnVarMap: TxnVarMap[K, V]
  ) extends TxnLogEntry[Map[K, V]] {

    override private[stm] def get: Map[K, V] =
      current

    override private[stm] def set(value: Map[K, V]): TxnLogEntry[Map[K, V]] =
      if (initial != value) {
        TxnLogUpdateVarMapStructureEntry(
          initial = initial,
          current = value,
          txnVarMap = txnVarMap
        )
      } else {
        TxnLogReadOnlyVarMapStructureEntry(
          initial = initial,
          txnVarMap = txnVarMap
        )
      }

    override private[stm] def commit(implicit F: Concurrent[F]): F[Unit] =
      F.unit

    override private[stm] def isDirty(implicit F: Concurrent[F]): F[Boolean] =
      txnVarMap.get.map(_ != initial)

    override private[stm] def lock(implicit
        F: Concurrent[F]
    ): F[Option[Semaphore[F]]] =
      F.pure(Some(txnVarMap.commitLock))

    override private[stm] def getRegisterRetry(implicit
        F: Concurrent[F]
    ): F[Deferred[F, Unit] => F[Unit]] =
      F.pure(txnVarMap.registerRetry)

    override private[stm] def idClosure(implicit
        F: Concurrent[F]
    ): F[IdClosure] =
      F.pure {
        IdClosure(
          readIds = Set(txnVarMap.runtimeId),
          updatedIds = Set(txnVarMap.runtimeId)
        )
      }
  }

  // See above comment for RO entry
  private[stm] case class TxnLogReadOnlyVarMapEntry[K, V](
      key: K,
      initial: Option[V],
      txnVarMap: TxnVarMap[K, V]
  ) extends TxnLogEntry[Option[V]] {

    override private[stm] def get: Option[V] =
      initial

    override private[stm] def set(value: Option[V]): TxnLogEntry[Option[V]] =
      if (initial != value) {
        TxnLogUpdateVarMapEntry(
          key = key,
          initial = initial,
          current = value,
          txnVarMap = txnVarMap
        )
      } else {
        this
      }

    override private[stm] def commit(implicit F: Concurrent[F]): F[Unit] =
      F.unit

    override private[stm] def isDirty(implicit F: Concurrent[F]): F[Boolean] =
      F.pure(false)

    override private[stm] def lock(implicit
        F: Concurrent[F]
    ): F[Option[Semaphore[F]]] =
      F.pure(None)

    override private[stm] def getRegisterRetry(implicit
        F: Concurrent[F]
    ): F[Deferred[F, Unit] => F[Unit]] =
      for {
        oTxnVar <- txnVarMap.getTxnVar(key)
        result <- oTxnVar match {
                    case Some(txnVar) =>
                      F.pure(i => txnVar.registerRetry(i))
                    case None =>
                      F.pure(i => txnVarMap.registerRetry(i))
                  }
      } yield result

    override private[stm] def idClosure(implicit
        F: Concurrent[F]
    ): F[IdClosure] =
      txnVarMap.getRuntimeId(key).map { rids =>
        IdClosure(
          readIds = rids.toSet,
          updatedIds = Set()
        )
      }
  }

  private[stm] case class TxnLogUpdateVarMapEntry[K, V](
      key: K,
      initial: Option[V],
      current: Option[V],
      txnVarMap: TxnVarMap[K, V]
  ) extends TxnLogEntry[Option[V]] {

    override private[stm] def get: Option[V] =
      current

    override private[stm] def set(value: Option[V]): TxnLogEntry[Option[V]] =
      if (initial != value) {
        TxnLogUpdateVarMapEntry(
          key = key,
          initial = initial,
          current = value,
          txnVarMap = txnVarMap
        )
      } else {
        TxnLogReadOnlyVarMapEntry(
          key = key,
          initial = initial,
          txnVarMap = txnVarMap
        )
      }

    override private[stm] def commit(implicit F: Concurrent[F]): F[Unit] =
      (initial, current) match {
        case (_, Some(cValue)) =>
          txnVarMap.addOrUpdate(key, cValue)
        case (Some(_), None) =>
          txnVarMap.delete(key)
        case _ =>
          F.unit
      }

    override private[stm] def isDirty(implicit F: Concurrent[F]): F[Boolean] =
      txnVarMap.get(key).map { oValue =>
        initial
          .map(iValue => !oValue.contains(iValue))
          .getOrElse(oValue.isDefined)
      }

    override private[stm] def lock(implicit
        F: Concurrent[F]
    ): F[Option[Semaphore[F]]] =
      for {
        oTxnVar <- txnVarMap.getTxnVar(key)
      } yield oTxnVar.map(_.commitLock)

    override private[stm] def getRegisterRetry(implicit
        F: Concurrent[F]
    ): F[Deferred[F, Unit] => F[Unit]] =
      for {
        oTxnVar <- txnVarMap.getTxnVar(key)
        result <- oTxnVar match {
                    case Some(txnVar) =>
                      F.pure { i: Deferred[F, Unit] =>
                        for {
                          _ <- txnVar.registerRetry(i)
                          _ <- txnVarMap.registerRetry(i)
                        } yield ()
                      }
                    case None =>
                      F.pure(i => txnVarMap.registerRetry(i))
                  }
      } yield result

    override private[stm] def idClosure(implicit
        F: Concurrent[F]
    ): F[IdClosure] =
      txnVarMap.getRuntimeId(key).map { rids =>
        val ridSet = rids.toSet
        if (
          (initial.isDefined && current.isEmpty) || (initial.isEmpty && current.isDefined)
        ) {
          IdClosure(
            readIds = ridSet + txnVarMap.runtimeId,
            updatedIds = ridSet + txnVarMap.runtimeId
          )
        } else {
          IdClosure(
            readIds = ridSet,
            updatedIds = ridSet
          )
        }
      }
  }

  private[stm] sealed trait TxnLog { self =>

    @nowarn
    private[stm] def getCurrentValue[V](txnVar: TxnVar[V])(implicit
        F: Concurrent[F]
    ): F[Option[V]] =
      F.pure(None)

    @nowarn
    private[stm] def getCurrentValue[K, V](key: K, txnVarMap: TxnVarMap[K, V])(
        implicit F: Concurrent[F]
    ): F[Option[V]] =
      F.pure(None)

    @nowarn
    private[stm] def getCurrentValue[K, V](txnVarMap: TxnVarMap[K, V])(implicit
        F: Concurrent[F]
    ): F[Map[K, V]] =
      F.pure(Map())

    @nowarn
    private[stm] def getVar[V](txnVar: TxnVar[V])(implicit
        F: Concurrent[F]
    ): F[TxnLog] =
      F.pure(self)

    @nowarn
    private[stm] def setVar[V](newValue: V, txnVar: TxnVar[V])(implicit
        F: Concurrent[F]
    ): F[TxnLog] =
      F.pure(self)

    @nowarn
    private[stm] def getVarMapValue[K, V](key: K, txnVarMap: TxnVarMap[K, V])(
        implicit F: Concurrent[F]
    ): F[TxnLog] =
      F.pure(self)

    @nowarn
    private[stm] def getVarMap[K, V](
        txnVarMap: TxnVarMap[K, V]
    )(implicit
        F: Concurrent[F]
    ): F[TxnLog] =
      F.pure(self)

    @nowarn
    private[stm] def setVarMap[K, V](
        newMap: Map[K, V],
        txnVarMap: TxnVarMap[K, V]
    )(implicit F: Concurrent[F]): F[TxnLog] =
      F.pure(self)

    @nowarn
    private[stm] def setVarMapValue[K, V](
        key: K,
        newValue: V,
        txnVarMap: TxnVarMap[K, V]
    )(implicit
        F: Concurrent[F]
    ): F[TxnLog] =
      F.pure(self)

    @nowarn
    private[stm] def modifyVarMapValue[K, V](
        key: K,
        f: V => V,
        txnVarMap: TxnVarMap[K, V]
    )(implicit
        F: Concurrent[F]
    ): F[TxnLog] =
      F.pure(self)

    @nowarn
    private[stm] def deleteVarMapValue[K, V](
        key: K,
        txnVarMap: TxnVarMap[K, V]
    )(implicit
        F: Concurrent[F]
    ): F[TxnLog] =
      F.pure(self)

    @nowarn
    private[stm] def raiseError(ex: Throwable)(implicit
        F: Concurrent[F]
    ): F[TxnLog] =
      F.pure(self)

    private[stm] def scheduleRetry(implicit F: Concurrent[F]): F[TxnLog] =
      F.pure(self)

    private[stm] def isDirty(implicit F: Concurrent[F]): F[Boolean] =
      F.pure(false)

    private[stm] def getRetrySignal(implicit
        F: Concurrent[F]
    ): F[Option[Deferred[F, Unit]]] =
      F.pure(None)

    private[stm] def commit(implicit F: Concurrent[F]): F[Unit] =
      F.unit

    private[stm] def idClosure(implicit F: Concurrent[F]): F[IdClosure] =
      F.pure(IdClosure.empty)

    @nowarn
    private[stm] def withLock[A](
        fa: F[A]
    )(implicit F: Concurrent[F]): F[A] =
      fa
  }

  private[stm] case class TxnLogValid(log: Map[TxnVarRuntimeId, TxnLogEntry[_]])
      extends TxnLog {

    override private[stm] def getCurrentValue[V](txnVar: TxnVar[V])(implicit
        F: Concurrent[F]
    ): F[Option[V]] =
      F.pure(log.get(txnVar.runtimeId).map(_.get.asInstanceOf[V]))

    // Retrieve the value from the log. If there is no log entry
    // try to retrieve from the original Txn Map
    override private[stm] def getCurrentValue[K, V](
        key: K,
        txnVarMap: TxnVarMap[K, V]
    )(implicit F: Concurrent[F]): F[Option[V]] =
      for {
        rids <- txnVarMap.getRuntimeId(key)
        logEntries = rids.flatMap(log.get)
        result <- if (logEntries.nonEmpty) {
                    F.pure(
                      logEntries.head.get.asInstanceOf[Option[V]]
                    )
                  } else {
                    txnVarMap.get(key)
                  }
      } yield result

    override private[stm] def getCurrentValue[K, V](
        txnVarMap: TxnVarMap[K, V]
    )(implicit F: Concurrent[F]): F[Map[K, V]] = {
      val logEntries = log.values.flatMap {
        case TxnLogReadOnlyVarMapEntry(key, Some(initial), entryMap)
            if txnVarMap.id == entryMap.id =>
          Some(key -> initial)
        case TxnLogUpdateVarMapEntry(key, _, Some(current), entryMap)
            if txnVarMap.id == entryMap.id =>
          Some(key -> current)
        case _ =>
          None
      }.toMap.asInstanceOf[Map[K, V]]

      if (log.contains(txnVarMap.runtimeId)) {
        F.pure(logEntries)
      } else {
        txnVarMap.get.map(_ ++ logEntries)
      }
    }

    override private[stm] def getVar[V](
        txnVar: TxnVar[V]
    )(implicit F: Concurrent[F]): F[TxnLog] =
      log.get(txnVar.runtimeId) match {
        case Some(_) =>
          F.pure(this)
        case None =>
          for {
            v <- txnVar.get
          } yield this.copy(
            log + (txnVar.runtimeId -> TxnLogReadOnlyVarEntry(v, txnVar))
          )
      }

    override private[stm] def setVar[V](newValue: V, txnVar: TxnVar[V])(implicit
        F: Concurrent[F]
    ): F[TxnLog] =
      log
        .get(txnVar.runtimeId)
        .map { entry =>
          F.pure(
            this.copy(
              log + (txnVar.runtimeId -> entry
                .asInstanceOf[TxnLogEntry[V]]
                .set(newValue))
            )
          )
        }
        .getOrElse {
          txnVar.get.map { v =>
            this.copy(
              log + (txnVar.runtimeId -> TxnLogUpdateVarEntry(v,
                                                              newValue,
                                                              txnVar
              ))
            )
          }
        }
        .map(_.asInstanceOf[TxnLog])

    private def getVarMapValueEntry[K, V](
        key: K,
        txnVarMap: TxnVarMap[K, V]
    )(implicit
        F: Concurrent[F]
    ): F[Option[(TxnVarRuntimeId, TxnLogEntry[Option[V]])]] =
      for {
        oTxnVar <- txnVarMap.getTxnVar(key)
        result <- oTxnVar match {
                    case Some(txnVar) =>
                      log.get(txnVar.runtimeId) match {
                        case Some(res) =>
                          F.pure(
                            Some(
                              (txnVar.runtimeId,
                               res.asInstanceOf[TxnLogEntry[Option[V]]]
                              )
                            )
                          )
                        case None =>
                          for {
                            txnVal <- txnVar.get
                          } yield Some(
                            (txnVar.runtimeId,
                             TxnLogReadOnlyVarMapEntry(
                               key,
                               Some(txnVal),
                               txnVarMap
                             )
                            )
                          )
                      }
                    case None =>
                      txnVarMap.getRuntimeId(key).map { rids =>
                        rids.flatMap(rid => log.get(rid).map((rid, _))) match {
                          case (rid, entry) :: _ =>
                            Some(
                              (rid,
                               entry
                                 .asInstanceOf[TxnLogEntry[Option[V]]]
                              )
                            )
                          case _ =>
                            None
                        }
                      }
                  }
      } yield result

    override private[stm] def getVarMap[K, V](
        txnVarMap: TxnVarMap[K, V]
    )(implicit F: Concurrent[F]): F[TxnLog] = {
      lazy val individualEntries: F[Map[TxnVarRuntimeId, TxnLogEntry[_]]] =
        for {
          preTxnEntries <- if (!log.contains(txnVarMap.runtimeId)) {
                             for {
                               oldMap <- txnVarMap.get
                               preTxn <-
                                 oldMap.keySet.toList.parTraverse { ks =>
                                   getVarMapValueEntry(ks, txnVarMap)
                                 }
                             } yield preTxn
                           } else {
                             F.pure(List())
                           }
          currentEntries <- getCurrentValue(txnVarMap)
          reads <- currentEntries.keySet.toList.parTraverse { ks =>
                     getVarMapValueEntry(ks, txnVarMap)
                   }
        } yield (preTxnEntries ::: reads).flatten.toMap

      log.get(txnVarMap.runtimeId) match {
        case Some(_) => individualEntries.map(es => this.copy(log ++ es))
        case None =>
          for {
            v       <- txnVarMap.get
            entries <- individualEntries
          } yield this.copy(
            (log ++ entries) + (txnVarMap.runtimeId -> TxnLogReadOnlyVarMapStructureEntry(
              v,
              txnVarMap
            ))
          )
      }
    }

    override private[stm] def getVarMapValue[K, V](
        key: K,
        txnVarMap: TxnVarMap[K, V]
    )(implicit
        F: Concurrent[F]
    ): F[TxnLog] =
      for {
        oTxnVar <- txnVarMap.getTxnVar(key)
        result <- oTxnVar match {
                    case Some(txnVar) =>
                      log.get(txnVar.runtimeId) match {
                        case Some(_) =>
                          F.pure(this) //Noop
                        case None =>
                          for {
                            txnVal <- txnVar.get
                          } yield this.copy(
                            log + (txnVar.runtimeId -> TxnLogReadOnlyVarMapEntry(
                              key,
                              Some(txnVal),
                              txnVarMap
                            ))
                          )
                      }
                    case None =>
                      for {
                        rids <- txnVarMap.getRuntimeId(key)
                      } yield rids.flatMap(log.get) match {
                        case _ :: Nil =>
                          this //Noop
                        case _ =>
                          TxnLogError {
                            new RuntimeException(
                              s"Tried to read non-existent key $key in transactional map"
                            )
                          }
                      }
                  }
      } yield result

    private def setVarMapValueEntry[K, V](
        key: K,
        newValue: V,
        txnVarMap: TxnVarMap[K, V]
    )(implicit
        F: Concurrent[F]
    ): F[Option[(TxnVarRuntimeId, TxnLogEntry[Option[V]])]] =
      txnVarMap
        .getTxnVar(key)
        .flatMap {
          case Some(txnVar) =>
            log.get(txnVar.runtimeId) match {
              case Some(entry) =>
                F.pure(
                  Some(
                    (txnVar.runtimeId,
                     entry
                       .asInstanceOf[TxnLogEntry[Option[V]]]
                       .set(Some(newValue))
                    )
                  )
                )
              case None =>
                txnVar.get.map { v =>
                  if (v != newValue) {
                    Some(
                      (txnVar.runtimeId,
                       TxnLogUpdateVarMapEntry(key,
                                               Some(v),
                                               Some(newValue),
                                               txnVarMap
                       )
                      )
                    )
                  } else {
                    None
                  }
                }
            }
          case None =>
            // The txnVar may have been set to be created in this transaction
            txnVarMap.getRuntimeId(key).map { rids =>
              rids.flatMap(rid => log.get(rid).map((rid, _))) match {
                case (rid, entry) :: _ =>
                  Some(
                    (rid,
                     entry
                       .asInstanceOf[TxnLogEntry[Option[V]]]
                       .set(Some(newValue))
                    )
                  )
                case _ =>
                  Some(
                    (rids.head,
                     TxnLogUpdateVarMapEntry(key,
                                             None,
                                             Some(newValue),
                                             txnVarMap
                     )
                    )
                  )
              }
            }
        }

    private def deleteVarMapValueEntry[K, V](
        key: K,
        txnVarMap: TxnVarMap[K, V]
    )(implicit
        F: Concurrent[F]
    ): F[Option[(TxnVarRuntimeId, TxnLogEntry[Option[V]])]] =
      for {
        oTxnVar <- txnVarMap.getTxnVar(key)
        result <- oTxnVar match {
                    case Some(txnVar) =>
                      log.get(txnVar.runtimeId) match {
                        case Some(entry) =>
                          F.pure(
                            Some(
                              (txnVar.runtimeId,
                               entry
                                 .asInstanceOf[TxnLogEntry[Option[V]]]
                                 .set(None)
                              )
                            )
                          )
                        case None =>
                          for {
                            txnVal <- txnVar.get
                          } yield Some(
                            (txnVar.runtimeId,
                             TxnLogUpdateVarMapEntry(key,
                                                     Some(txnVal),
                                                     None,
                                                     txnVarMap
                             )
                            )
                          )
                      }
                    case None =>
                      for {
                        rids <- txnVarMap.getRuntimeId(key)
                      } yield rids.flatMap(rid =>
                        log.get(rid).map((rid, _))
                      ) match {
                        case (rid, entry) :: _ =>
                          Some(
                            (rid,
                             entry
                               .asInstanceOf[TxnLogEntry[Option[V]]]
                               .set(None)
                            )
                          )
                        case _ => // Should never happen
                          None
                      }
                  }
      } yield result

    override private[stm] def setVarMap[K, V](
        newMap: Map[K, V],
        txnVarMap: TxnVarMap[K, V]
    )(implicit F: Concurrent[F]): F[TxnLog] = {
      val individualEntries: F[Map[TxnVarRuntimeId, TxnLogEntry[_]]] = for {
        currentMap <- getCurrentValue(txnVarMap)
        deletions <-
          (currentMap.keySet -- newMap.keySet).toList.parTraverse { ks =>
            deleteVarMapValueEntry(ks, txnVarMap)
          }
        updates <- newMap.toList.parTraverse { kv =>
                     setVarMapValueEntry(kv._1, kv._2, txnVarMap)
                   }
      } yield (deletions ::: updates).flatten.toMap

      (log.get(txnVarMap.runtimeId) match {
        case Some(entry) =>
          individualEntries.map { entries =>
            this.copy(
              (log ++ entries) + (txnVarMap.runtimeId -> entry
                .asInstanceOf[TxnLogEntry[Map[K, V]]]
                .set(newMap))
            )
          }
        case _ =>
          for {
            v       <- txnVarMap.get
            entries <- individualEntries
          } yield this.copy(
            (log ++ entries) + (txnVarMap.runtimeId -> TxnLogUpdateVarMapStructureEntry(
              v,
              newMap,
              txnVarMap
            ))
          )
      }).map(_.asInstanceOf[TxnLog])
    }

    override private[stm] def setVarMapValue[K, V](
        key: K,
        newValue: V,
        txnVarMap: TxnVarMap[K, V]
    )(implicit F: Concurrent[F]): F[TxnLog] =
      txnVarMap
        .getTxnVar(key)
        .flatMap {
          case Some(txnVar) =>
            log.get(txnVar.runtimeId) match {
              case Some(entry) =>
                F.pure(
                  this.copy(
                    log + (txnVar.runtimeId -> entry
                      .asInstanceOf[TxnLogEntry[Option[V]]]
                      .set(Some(newValue)))
                  )
                )
              case None =>
                txnVar.get.map { v =>
                  if (v != newValue) {
                    this.copy(
                      log + (txnVar.runtimeId -> TxnLogUpdateVarMapEntry(
                        key,
                        Some(v),
                        Some(newValue),
                        txnVarMap
                      ))
                    )
                  } else {
                    this
                  }
                }
            }
          case None =>
            // The txnVar may have been set to be created in this transaction
            txnVarMap.getRuntimeId(key).map { rids =>
              rids.flatMap(rid => log.get(rid).map((rid, _))) match {
                case (rid, entry) :: _ =>
                  this.copy(
                    log + (rid -> entry
                      .asInstanceOf[TxnLogEntry[Option[V]]]
                      .set(Some(newValue)))
                  )
                case _ =>
                  this.copy(
                    log + (rids.head -> TxnLogUpdateVarMapEntry(key,
                                                                None,
                                                                Some(newValue),
                                                                txnVarMap
                    ))
                  )
              }
            }
        }
        .map(_.asInstanceOf[TxnLog])

    override private[stm] def modifyVarMapValue[K, V](
        key: K,
        f: V => V,
        txnVarMap: TxnVarMap[K, V]
    )(implicit F: Concurrent[F]): F[TxnLog] =
      txnVarMap
        .getTxnVar(key)
        .flatMap {
          case Some(txnVar) =>
            (log.get(txnVar.runtimeId) match {
              case Some(entry) =>
                F.pure {
                  entry.get.asInstanceOf[Option[V]] match {
                    case Some(v) =>
                      this.copy(
                        log + (txnVar.runtimeId -> entry
                          .asInstanceOf[TxnLogEntry[Option[V]]]
                          .set(Some(f(v))))
                      )
                    case None =>
                      TxnLogError {
                        new RuntimeException(
                          s"Key $key not found for modification"
                        )
                      }
                  }
                }
              case None =>
                txnVar.get.map { v =>
                  this.copy(
                    log + (txnVar.runtimeId -> TxnLogUpdateVarMapEntry(
                      key,
                      Some(v),
                      Some(f(v)),
                      txnVarMap
                    ))
                  )
                }
            }).map(_.asInstanceOf[TxnLog])
          case None =>
            // The txnVar may have been set to be created in this transaction
            txnVarMap
              .getRuntimeId(key)
              .map { rids =>
                rids.flatMap(rid => log.get(rid).map((rid, _))) match {
                  case (rid, entry) :: _ =>
                    val castEntry: TxnLogEntry[Option[V]] =
                      entry.asInstanceOf[TxnLogEntry[Option[V]]]
                    castEntry.get match {
                      case Some(v) =>
                        this.copy(
                          log + (rid -> castEntry
                            .set(Some(f(v))))
                        )
                      case None =>
                        TxnLogError {
                          new RuntimeException(
                            s"Key $key not found for modification"
                          )
                        }
                    }
                  case _ =>
                    TxnLogError {
                      new RuntimeException(
                        s"Key $key not found for modification"
                      )
                    }
                }
              }
              .map(_.asInstanceOf[TxnLog])
        }

    override private[stm] def deleteVarMapValue[K, V](
        key: K,
        txnVarMap: TxnVarMap[K, V]
    )(implicit
        F: Concurrent[F]
    ): F[TxnLog] =
      for {
        oTxnVar <- txnVarMap.getTxnVar(key)
        result <- oTxnVar match {
                    case Some(txnVar) =>
                      log.get(txnVar.runtimeId) match {
                        case Some(entry) =>
                          F.pure(
                            this.copy(
                              log + (txnVar.runtimeId -> entry
                                .asInstanceOf[TxnLogEntry[Option[V]]]
                                .set(None))
                            )
                          )
                        case None =>
                          for {
                            txnVal <- txnVar.get
                          } yield this.copy(
                            log + (txnVar.runtimeId ->
                              TxnLogUpdateVarMapEntry(key,
                                                      Some(txnVal),
                                                      None,
                                                      txnVarMap
                              ))
                          )
                      }
                    case None =>
                      for {
                        rids <- txnVarMap.getRuntimeId(key)
                      } yield rids.flatMap(rid =>
                        log.get(rid).map((rid, _))
                      ) match {
                        case (rid, entry) :: _ =>
                          this.copy(
                            log + (rid -> entry
                              .asInstanceOf[TxnLogEntry[Option[V]]]
                              .set(None))
                          )
                        case _ => // Throw error to be consistent with read behaviour
                          TxnLogError {
                            new RuntimeException(
                              s"Tried to remove non-existent key $key in transactional map"
                            )
                          }
                      }
                  }
      } yield result

    override private[stm] def raiseError(ex: Throwable)(implicit
        F: Concurrent[F]
    ): F[TxnLog] =
      F.pure(TxnLogError(ex))

    override private[stm] def scheduleRetry(implicit
        F: Concurrent[F]
    ): F[TxnLog] =
      F.pure(TxnLogRetry(this))

    // Favor computational efficiency over parallelism here by
    // using a fold to short-circuit a log check instead of
    // using existence in a parallel traversal. I.e. we have
    // enough parallelism in the runtime to ensure good hardware
    // utilisation
    override private[stm] def isDirty(implicit F: Concurrent[F]): F[Boolean] =
      log.values.toList.foldLeft(F.pure(false)) { (i, j) =>
        for {
          prev <- i
          result <- if (prev) {
                      F.pure(prev)
                    } else {
                      j.isDirty
                    }
        } yield result
      }

    override private[stm] def idClosure(implicit
        F: Concurrent[F]
    ): F[IdClosure] =
      log.values.toList.parTraverse { entry =>
        entry.idClosure
      }.map(_.reduce(_ mergeWith _))

    override private[stm] def withLock[A](
        fa: F[A]
    )(implicit F: Concurrent[F]): F[A] =
      for {
        locks <- log.values.toList.parTraverse(_.lock)
        result <- locks.toSet.flatten
                    .foldLeft(Resource.eval(F.unit))((i, j) => i >> j.permit)
                    .use(_ => fa)
      } yield result

    override private[stm] def commit(implicit F: Concurrent[F]): F[Unit] =
      log.values.toList.parTraverse(_.commit).void
  }

  private[stm] object TxnLogValid {
    private[stm] val empty: TxnLogValid = TxnLogValid(Map())
  }

  private[stm] case class TxnLogRetry(validLog: TxnLogValid) extends TxnLog {

    override private[stm] def isDirty(implicit F: Concurrent[F]): F[Boolean] =
      validLog.isDirty

    override private[stm] def getRetrySignal(implicit
        F: Concurrent[F]
    ): F[Option[Deferred[F, Unit]]] =
      for {
        retrySignal <- Deferred[F, Unit]
        registerRetries <-
          validLog.log.values.toList.parTraverse(_.getRegisterRetry)
        _ <- registerRetries.parTraverse(rr => rr(retrySignal))
      } yield Some(retrySignal)
  }

  private[stm] case class TxnLogError(ex: Throwable) extends TxnLog
}
