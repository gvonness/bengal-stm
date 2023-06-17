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
package bengal.stm.runtime

import bengal.stm.model._
import bengal.stm.model.runtime._

import cats.effect.Deferred
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Semaphore
import cats.syntax.all._

import scala.annotation.nowarn

private[stm] trait TxnLogContext[F[_]] {
  this: AsyncImplicits[F] =>

  private[stm] sealed trait TxnLogEntry[V] {
    private[stm] def get: V
    private[stm] def set(value: V): TxnLogEntry[V]
    private[stm] def commit: F[Unit]
    private[stm] def isDirty: F[Boolean]
    private[stm] def lock: F[Option[Semaphore[F]]]
    private[stm] def idClosure: F[IdClosure]

    private[stm] def getRegisterRetry: F[Deferred[F, Unit] => F[Unit]]
  }

  // RO entry is not necessary for pure transactions.
  // However, to make the library interface more
  // predictable in the face of side-effects, RO
  // entries are created
  private[stm] case class TxnLogReadOnlyVarEntry[V](
      initial: V,
      txnVar: TxnVar[F, V]
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

    override private[stm] lazy val commit: F[Unit] =
      Async[F].unit

    override private[stm] lazy val isDirty: F[Boolean] =
      Async[F].pure(false)

    override private[stm] lazy val lock: F[Option[Semaphore[F]]] =
      Async[F].pure(None)

    override private[stm] lazy val getRegisterRetry
        : F[Deferred[F, Unit] => F[Unit]] =
      Async[F].delay(txnVar.registerRetry)

    override private[stm] lazy val idClosure: F[IdClosure] =
      Async[F].delay {
        IdClosure(readIds = Set(txnVar.runtimeId), updatedIds = Set())
      }
  }

  private[stm] case class TxnLogUpdateVarEntry[V](
      initial: V,
      current: V,
      txnVar: TxnVar[F, V]
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

    override private[stm] lazy val commit: F[Unit] =
      txnVar.set(current)

    override private[stm] lazy val isDirty: F[Boolean] =
      txnVar.get.map(_ != initial)

    override private[stm] lazy val lock: F[Option[Semaphore[F]]] =
      Async[F].delay(Some(txnVar.commitLock))

    override private[stm] lazy val getRegisterRetry
        : F[Deferred[F, Unit] => F[Unit]] =
      Async[F].delay(txnVar.registerRetry)

    override private[stm] lazy val idClosure: F[IdClosure] =
      Async[F].delay(txnVar.runtimeId).map { rId =>
        IdClosure(
          readIds = Set(rId),
          updatedIds = Set(rId)
        )
      }
  }

  // See above comment for RO entry
  private[stm] case class TxnLogReadOnlyVarMapStructureEntry[K, V](
      initial: Map[K, V],
      txnVarMap: TxnVarMap[F, K, V]
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

    override private[stm] lazy val commit: F[Unit] =
      Async[F].unit

    override private[stm] lazy val isDirty: F[Boolean] =
      Async[F].pure(false)

    override private[stm] lazy val lock: F[Option[Semaphore[F]]] =
      Async[F].pure(None)

    override private[stm] lazy val getRegisterRetry
        : F[Deferred[F, Unit] => F[Unit]] =
      Async[F].delay(txnVarMap.registerRetry)

    override private[stm] lazy val idClosure: F[IdClosure] =
      Async[F].delay {
        IdClosure(
          readIds = Set(txnVarMap.runtimeId),
          updatedIds = Set()
        )
      }
  }

  private[stm] case class TxnLogUpdateVarMapStructureEntry[K, V](
      initial: Map[K, V],
      current: Map[K, V],
      txnVarMap: TxnVarMap[F, K, V]
  ) extends TxnLogEntry[Map[K, V]] {

    override private[stm] lazy val get: Map[K, V] =
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

    override private[stm] lazy val commit: F[Unit] =
      Async[F].unit

    override private[stm] lazy val isDirty: F[Boolean] =
      txnVarMap.get.map(_ != initial)

    override private[stm] lazy val lock: F[Option[Semaphore[F]]] =
      Async[F].delay(Some(txnVarMap.commitLock))

    override private[stm] lazy val getRegisterRetry
        : F[Deferred[F, Unit] => F[Unit]] =
      Async[F].delay(txnVarMap.registerRetry)

    override private[stm] lazy val idClosure: F[IdClosure] =
      Async[F].delay {
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
      txnVarMap: TxnVarMap[F, K, V]
  ) extends TxnLogEntry[Option[V]] {

    override private[stm] lazy val get: Option[V] =
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

    override private[stm] lazy val commit: F[Unit] =
      Async[F].unit

    override private[stm] lazy val isDirty: F[Boolean] =
      Async[F].pure(false)

    override private[stm] lazy val lock: F[Option[Semaphore[F]]] =
      Async[F].pure(None)

    override private[stm] lazy val getRegisterRetry
        : F[Deferred[F, Unit] => F[Unit]] =
      for {
        oTxnVar <- txnVarMap.getTxnVar(key)
        result <- oTxnVar match {
                    case Some(txnVar) =>
                      Async[F].pure(i => txnVar.registerRetry(i))
                    case None =>
                      Async[F].pure(i => txnVarMap.registerRetry(i))
                  }
      } yield result

    override private[stm] lazy val idClosure: F[IdClosure] =
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
      txnVarMap: TxnVarMap[F, K, V]
  ) extends TxnLogEntry[Option[V]] {

    override private[stm] lazy val get: Option[V] =
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

    override private[stm] lazy val commit: F[Unit] =
      (initial, current) match {
        case (_, Some(cValue)) =>
          txnVarMap.addOrUpdate(key, cValue)
        case (Some(_), None) =>
          txnVarMap.delete(key)
        case _ =>
          Async[F].unit
      }

    override private[stm] lazy val isDirty: F[Boolean] =
      txnVarMap.get(key).map { oValue =>
        initial
          .map(iValue => !oValue.contains(iValue))
          .getOrElse(oValue.isDefined)
      }

    override private[stm] lazy val lock: F[Option[Semaphore[F]]] =
      for {
        oTxnVar <- txnVarMap.getTxnVar(key)
      } yield oTxnVar.map(_.commitLock)

    override private[stm] lazy val getRegisterRetry
        : F[Deferred[F, Unit] => F[Unit]] =
      for {
        oTxnVar <- txnVarMap.getTxnVar(key)
        result <- oTxnVar match {
                    case Some(txnVar) =>
                      Async[F].delay { i: Deferred[F, Unit] =>
                        for {
                          _ <- txnVar.registerRetry(i)
                          _ <- txnVarMap.registerRetry(i)
                        } yield ()
                      }
                    case None =>
                      Async[F].delay(i => txnVarMap.registerRetry(i))
                  }
      } yield result

    override private[stm] lazy val idClosure: F[IdClosure] = {
      for {
        ridSet <- txnVarMap.getRuntimeId(key).map(_.toSet)
        result <-
          Async[F].ifM(
            Async[F].delay(
              (initial.isDefined && current.isEmpty) || (initial.isEmpty && current.isDefined)
            )
          )(
            for {
              newRidSet <- Async[F].delay(txnVarMap.runtimeId).map(ridSet + _)
            } yield IdClosure(
              readIds = ridSet,
              updatedIds = newRidSet
            ),
            Async[F].delay(
              IdClosure(
                readIds = ridSet,
                updatedIds = ridSet
              )
            )
          )
      } yield result
    }
  }

  private[stm] sealed trait TxnLog { self =>

    private[stm] def getVar[V](txnVar: TxnVar[F, V]): F[(TxnLog, V)]

    @nowarn
    private[stm] def delay[V](value: F[V]): F[(TxnLog, V)] =
      Async[F].delay(self, ().asInstanceOf[V])

    @nowarn
    private[stm] def pure[V](value: V): F[(TxnLog, V)] =
      Async[F].pure(self, ().asInstanceOf[V])

    @nowarn
    private[stm] def setVar[V](
        newValue: F[V],
        txnVar: TxnVar[F, V]
    ): F[TxnLog] =
      Async[F].pure(self)

    @nowarn
    private[stm] def getVarMapValue[K, V](
        key: F[K],
        txnVarMap: TxnVarMap[F, K, V]
    ): F[(TxnLog, Option[V])] =
      Async[F].pure((self, None))

    @nowarn
    private[stm] def getVarMap[K, V](
        txnVarMap: TxnVarMap[F, K, V]
    ): F[(TxnLog, Map[K, V])] =
      Async[F].pure(self, Map())

    @nowarn
    private[stm] def setVarMap[K, V](
        newMap: F[Map[K, V]],
        txnVarMap: TxnVarMap[F, K, V]
    ): F[TxnLog] =
      Async[F].pure(self)

    @nowarn
    private[stm] def setVarMapValue[K, V](
        key: F[K],
        newValue: F[V],
        txnVarMap: TxnVarMap[F, K, V]
    ): F[TxnLog] =
      Async[F].pure(self)

    @nowarn
    private[stm] def modifyVarMapValue[K, V](
        key: F[K],
        f: V => F[V],
        txnVarMap: TxnVarMap[F, K, V]
    ): F[TxnLog] =
      Async[F].pure(self)

    @nowarn
    private[stm] def deleteVarMapValue[K, V](
        key: F[K],
        txnVarMap: TxnVarMap[F, K, V]
    ): F[TxnLog] =
      Async[F].pure(self)

    @nowarn
    private[stm] def raiseError(ex: Throwable): F[TxnLog] =
      Async[F].pure(self)

    private[stm] def scheduleRetry: F[TxnLog]

    private[stm] def isDirty: F[Boolean]

    private[stm] def getRetrySignal: F[Option[Deferred[F, Unit]]]

    private[stm] def commit: F[Unit]

    private[stm] def idClosure: F[IdClosure]

    private[stm] def withLock[A](fa: F[A]): F[A] =
      fa
  }

  private[stm] case class TxnLogValid(log: Map[TxnVarRuntimeId, TxnLogEntry[_]])
      extends TxnLog {

    import TxnLogValid._

    override private[stm] def delay[V](
        value: F[V]
    ): F[(TxnLog, V)] =
      value
        .map((this.asInstanceOf[TxnLog], _))
        .handleErrorWith { ex =>
          Async[F].delay((TxnLogError(ex), ().asInstanceOf[V]))
        }

    override private[stm] def pure[V](value: V): F[(TxnLog, V)] =
      Async[F].pure((this, value))

    override private[stm] def getVar[V](
        txnVar: TxnVar[F, V]
    ): F[(TxnLog, V)] = {
      def fallbackF(rId: TxnVarRuntimeId): F[(TxnLog, V)] =
        for {
          v <- txnVar.get
          newLog <-
            Async[F].delay(
              log + (rId -> TxnLogReadOnlyVarEntry(v, txnVar))
            )
          result <- Async[F].delay((TxnLogValid(newLog), v))
        } yield result

      for {
        rId <- Async[F].delay(txnVar.runtimeId)
        logEntry <- Async[F].delay(log.get(rId))
        result <- logEntry match {
          case Some(entry) =>
            Async[F].delay((this, entry.get.asInstanceOf[V]))
          case None =>
            fallbackF(rId)
        }
      } yield result
    }

    override private[stm] def setVar[V](
        newValue: F[V],
        txnVar: TxnVar[F, V]
    ): F[TxnLog] =
      (for {
        materializedValue <- newValue
        rId <- Async[F].delay(txnVar.runtimeId)
        entry <- Async[F].delay(log.get(rId))
        rawResult <- entry match {
          case Some(entry) =>
            Async[F].delay(log + (txnVar.runtimeId -> entry
              .asInstanceOf[TxnLogEntry[V]]
              .set(materializedValue))).map(TxnLogValid(_))
          case _ =>
            for {
              v <- txnVar.get
              rId <- Async[F].delay(txnVar.runtimeId)
              newValue <- Async[F].delay(TxnLogUpdateVarEntry(
                v,
                materializedValue,
                txnVar
              ))
              newLog <- Async[F].delay(log + (rId -> newValue))
            } yield TxnLogValid(newLog)
        }
        result <- Async[F].delay(rawResult.asInstanceOf[TxnLog])
      } yield result).handleErrorWith(raiseError)

    private def getVarMapValueEntry[K, V](
        key: K,
        txnVarMap: TxnVarMap[F, K, V]
    ): F[Option[(TxnVarRuntimeId, TxnLogEntry[Option[V]])]] =
      for {
        oTxnVar <- txnVarMap.getTxnVar(key)
        result <- oTxnVar match {
                    case Some(txnVar) =>
                      log.get(txnVar.runtimeId) match {
                        case Some(res) =>
                          Async[F].delay(
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
        txnVarMap: TxnVarMap[F, K, V]
    ): F[(TxnLog, Map[K, V])] = {
      lazy val individualEntries: F[Map[TxnVarRuntimeId, TxnLogEntry[_]]] =
        for {
          preTxnEntries <-
            Async[F].ifM(Async[F].delay(!log.contains(txnVarMap.runtimeId)))(
              for {
                oldMap <- txnVarMap.get
                preTxn <-
                  oldMap.keySet.toList.traverse { ks =>
                    getVarMapValueEntry(ks, txnVarMap)
                  }
              } yield preTxn,
              Async[F].pure(
                List[Option[(TxnVarRuntimeId, TxnLogEntry[Option[V]])]]()
              )
            )
          currentEntries <- extractMap(txnVarMap, log)
          reads <- currentEntries.keySet.toList.traverse { ks =>
                     getVarMapValueEntry(ks, txnVarMap)
                   }
        } yield (preTxnEntries ::: reads).flatten.toMap

      log.get(txnVarMap.runtimeId) match {
        case Some(_) =>
          for {
            entries <- individualEntries
            newLog <- Async[F].delay(log ++ entries)
            newMap <- extractMap(txnVarMap, newLog)
          } yield (this.copy(newLog), newMap)
        case None =>
          for {
            v       <- txnVarMap.get
            entries <- individualEntries
            newLog <- Async[F].delay(
              (log ++ entries) + (txnVarMap.runtimeId -> TxnLogReadOnlyVarMapStructureEntry(
                v,
                txnVarMap
              )))
            newMap <- extractMap(txnVarMap, newLog)
          } yield (this.copy(newLog), newMap)
      }
    }

    override private[stm] def getVarMapValue[K, V](
        key: F[K],
        txnVarMap: TxnVarMap[F, K, V]
    ): F[(TxnLog, Option[V])] =
      (for {
        materializedKey <- key
        oTxnVar         <- txnVarMap.getTxnVar(materializedKey)
        result <- (oTxnVar match {
                    case Some(txnVar) =>
                      log.get(txnVar.runtimeId) match {
                        case Some(entry) =>
                          Async[F].delay(
                            (this, entry.get)
                          ) //Noop
                        case None =>
                          for {
                            txnVal <- txnVar.get
                          } yield (this.copy(
                                     log + (txnVar.runtimeId -> TxnLogReadOnlyVarMapEntry(
                                       materializedKey,
                                       Some(txnVal),
                                       txnVarMap
                                     ))
                                   ),
                                   Some(txnVal)
                          )
                      }
                    case None =>
                      for {
                        rids <- txnVarMap.getRuntimeId(
                                  materializedKey
                                )
                      } yield rids.flatMap(log.get) match {
                        case entry :: Nil =>
                          (this, entry.get) //Noop
                        case _ =>
                          (this, None)
                      }
                  }).map { case (log, value) =>
                    (log.asInstanceOf[TxnLog], value.asInstanceOf[Option[V]])
                  }
      } yield result).handleErrorWith { ex =>
        raiseError(ex).map(log => (log, None))
      }

    private def setVarMapValueEntry[K, V](
        key: K,
        newValue: V,
        txnVarMap: TxnVarMap[F, K, V]
    ): F[Option[(TxnVarRuntimeId, TxnLogEntry[Option[V]])]] =
      txnVarMap
        .getTxnVar(key)
        .flatMap {
          case Some(txnVar) =>
            log.get(txnVar.runtimeId) match {
              case Some(entry) =>
                Async[F].delay(
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
        txnVarMap: TxnVarMap[F, K, V]
    ): F[Option[(TxnVarRuntimeId, TxnLogEntry[Option[V]])]] =
      for {
        oTxnVar <- txnVarMap.getTxnVar(key)
        result <- oTxnVar match {
                    case Some(txnVar) =>
                      log.get(txnVar.runtimeId) match {
                        case Some(entry) =>
                          Async[F].delay(
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
        newMap: F[Map[K, V]],
        txnVarMap: TxnVarMap[F, K, V]
    ): F[TxnLog] =
      newMap.flatMap { materializedNewMap =>
        val individualEntries: F[Map[TxnVarRuntimeId, TxnLogEntry[_]]] = for {
          currentMap <- extractMap(txnVarMap, log)
          deletions <-
            (currentMap.keySet -- materializedNewMap.keySet).toList.traverse {
              ks =>
                deleteVarMapValueEntry(ks, txnVarMap)
            }
          updates <- materializedNewMap.toList.traverse { kv =>
                       setVarMapValueEntry(kv._1, kv._2, txnVarMap)
                     }
        } yield (deletions ::: updates).flatten.toMap

        (log.get(txnVarMap.runtimeId) match {
          case Some(entry) =>
            individualEntries.map { entries =>
              this.copy(
                (log ++ entries) + (txnVarMap.runtimeId -> entry
                  .asInstanceOf[TxnLogEntry[Map[K, V]]]
                  .set(materializedNewMap))
              )
            }
          case _ =>
            for {
              v       <- txnVarMap.get
              entries <- individualEntries
            } yield this.copy(
              (log ++ entries) + (txnVarMap.runtimeId -> TxnLogUpdateVarMapStructureEntry(
                v,
                materializedNewMap,
                txnVarMap
              ))
            )
        }).map(_.asInstanceOf[TxnLog])
      }
        .handleErrorWith(raiseError)

    override private[stm] def setVarMapValue[K, V](
        key: F[K],
        newValue: F[V],
        txnVarMap: TxnVarMap[F, K, V]
    ): F[TxnLog] =
      (for {
        materializedKey      <- key
        materializedNewValue <- newValue
        result <- txnVarMap
                    .getTxnVar(materializedKey)
                    .flatMap {
                      case Some(txnVar) =>
                        log.get(txnVar.runtimeId) match {
                          case Some(entry) =>
                            Async[F].delay(
                              this.copy(
                                log + (txnVar.runtimeId -> entry
                                  .asInstanceOf[TxnLogEntry[Option[V]]]
                                  .set(Some(materializedNewValue)))
                              )
                            )
                          case None =>
                            txnVar.get.map { v =>
                              if (v != materializedNewValue) {
                                this.copy(
                                  log + (txnVar.runtimeId -> TxnLogUpdateVarMapEntry(
                                    materializedKey,
                                    Some(v),
                                    Some(materializedNewValue),
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
                        txnVarMap.getRuntimeId(materializedKey).map { rids =>
                          rids
                            .flatMap(rid => log.get(rid).map((rid, _))) match {
                            case (rid, entry) :: _ =>
                              this.copy(
                                log + (rid -> entry
                                  .asInstanceOf[TxnLogEntry[Option[V]]]
                                  .set(Some(materializedNewValue)))
                              )
                            case _ =>
                              this.copy(
                                log + (rids.head -> TxnLogUpdateVarMapEntry(
                                  materializedKey,
                                  None,
                                  Some(materializedNewValue),
                                  txnVarMap
                                ))
                              )
                          }
                        }
                    }
                    .map(_.asInstanceOf[TxnLog])
      } yield result)
        .handleErrorWith(raiseError)

    override private[stm] def modifyVarMapValue[K, V](
        key: F[K],
        f: V => F[V],
        txnVarMap: TxnVarMap[F, K, V]
    ): F[TxnLog] =
      key.flatMap { materializedKey =>
        txnVarMap
          .getTxnVar(materializedKey)
          .flatMap {
            case Some(txnVar) =>
              (log.get(txnVar.runtimeId) match {
                case Some(entry) =>
                  entry.get.asInstanceOf[Option[V]] match {
                    case Some(v) =>
                      f(v).map { innerResult =>
                        this.copy(
                          log + (txnVar.runtimeId -> entry
                            .asInstanceOf[TxnLogEntry[Option[V]]]
                            .set(Some(innerResult)))
                        )
                      }
                    case None =>
                      Async[F].delay {
                        TxnLogError(
                          new RuntimeException(
                            s"Key $materializedKey not found for modification"
                          )
                        )
                      }
                  }
                case None =>
                  for {
                    v           <- txnVar.get
                    innerResult <- f(v)
                  } yield this.copy(
                    log + (txnVar.runtimeId -> TxnLogUpdateVarMapEntry(
                      materializedKey,
                      Some(v),
                      Some(innerResult),
                      txnVarMap
                    ))
                  )
              }).map(_.asInstanceOf[TxnLog])
            case None =>
              // The txnVar may have been set to be created in this transaction
              txnVarMap
                .getRuntimeId(materializedKey)
                .flatMap { rids =>
                  (rids.flatMap(rid => log.get(rid).map((rid, _))) match {
                    case (rid, entry) :: _ =>
                      val castEntry: TxnLogEntry[Option[V]] =
                        entry.asInstanceOf[TxnLogEntry[Option[V]]]
                      castEntry.get match {
                        case Some(v) =>
                          f(v).map { innerResult =>
                            this.copy(
                              log + (rid -> castEntry
                                .set(Some(innerResult)))
                            )
                          }
                        case None =>
                          Async[F].delay {
                            TxnLogError(
                              new RuntimeException(
                                s"Key $materializedKey not found for modification"
                              )
                            )
                          }
                      }
                    case _ =>
                      Async[F].delay {
                        TxnLogError(
                          new RuntimeException(
                            s"Key $materializedKey not found for modification"
                          )
                        )
                      }
                  }).map(_.asInstanceOf[TxnLog])
                }
          }
      }.handleErrorWith(raiseError)

    override private[stm] def deleteVarMapValue[K, V](
        key: F[K],
        txnVarMap: TxnVarMap[F, K, V]
    ): F[TxnLog] =
      (for {
        materializedKey <- key
        oTxnVar         <- txnVarMap.getTxnVar(materializedKey)
        result <- oTxnVar match {
                    case Some(txnVar) =>
                      log.get(txnVar.runtimeId) match {
                        case Some(entry) =>
                          Async[F].delay(
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
                              TxnLogUpdateVarMapEntry(materializedKey,
                                                      Some(txnVal),
                                                      None,
                                                      txnVarMap
                              ))
                          )
                      }
                    case None =>
                      for {
                        rids <- txnVarMap.getRuntimeId(
                                  materializedKey
                                )
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
                              s"Tried to remove non-existent key $materializedKey in transactional map"
                            )
                          }
                      }
                  }
      } yield result.asInstanceOf[TxnLog])
        .handleErrorWith(raiseError)

    override private[stm] def raiseError(ex: Throwable): F[TxnLog] =
      Async[F].delay(TxnLogError(ex))

    // We throw here to short-circuit the Free compiler recursion.
    // There is no point in processing anything else beyond the retry,
    // which could lead to impossible casts being attempted, which would
    // just throw anyway.
    // Note that we do not throw on `raiseError` as the Txn may contain
    // a handleError entry; i.e. we can not simply short-circuit the
    // recursion.
    override private[stm] def scheduleRetry: F[TxnLog] =
      throw TxnRetryException(this)

    // Favor computational efficiency over parallelism here by
    // using a fold to short-circuit a log check instead of
    // using existence in a parallel traversal. I.e. we have
    // enough parallelism in the runtime to ensure good hardware
    // utilisation
    override private[stm] lazy val isDirty: F[Boolean] =
      log.values.toList.foldLeft(Async[F].pure(false)) { (i, j) =>
        for {
          prev <- i
          result <- if (prev) {
                      Async[F].pure(prev)
                    } else {
                      j.isDirty
                    }
        } yield result
      }

    override private[stm] lazy val idClosure: F[IdClosure] =
      log.values.toList.traverse { entry =>
        entry.idClosure
      }.map(_.reduce(_ mergeWith _))

    override private[stm] def withLock[A](fa: F[A]): F[A] =
      for {
        locks <- log.values.toList.traverse(_.lock)
        result <-
          locks.toSet.flatten
            .foldLeft(Resource.eval(Async[F].unit))((i, j) => i >> j.permit)
            .use(_ => fa)
      } yield result

    override private[stm] lazy val commit: F[Unit] =
      log.values.toList.traverse(_.commit).void

    override private[stm] lazy val getRetrySignal
        : F[Option[Deferred[F, Unit]]] =
      Async[F].pure(None)
  }

  private[stm] object TxnLogValid {
    private[stm] val empty: TxnLogValid = TxnLogValid(Map())

    private def extractMap[K, V](
        txnVarMap: TxnVarMap[F, K, V],
        log: Map[TxnVarRuntimeId, TxnLogEntry[_]]
    ): F[Map[K, V]] = {
      val logEntriesF =
        Async[F].delay(log.values.flatMap {
          case TxnLogReadOnlyVarMapEntry(key, Some(initial), entryMap)
              if txnVarMap.id == entryMap.id =>
            Some(key -> initial)
          case TxnLogUpdateVarMapEntry(key, _, Some(current), entryMap)
              if txnVarMap.id == entryMap.id =>
            Some(key -> current)
          case _ =>
            None
        }.toMap.asInstanceOf[Map[K, V]])

      Async[F].ifM(Async[F].delay(log.contains(txnVarMap.runtimeId)))(
        logEntriesF,
        for {
          logEntries   <- logEntriesF
          txnVarMapGet <- txnVarMap.get
        } yield txnVarMapGet ++ logEntries
      )
    }
  }

  private[stm] case class TxnLogRetry(validLog: TxnLogValid) extends TxnLog {

    override private[stm] def getVar[V](
        txnVar: TxnVar[F, V]
    ): F[(TxnLog, V)] =
      validLog.log.get(txnVar.runtimeId) match {
        case Some(entry) =>
          Async[F].delay((this, entry.get.asInstanceOf[V]))
        case None =>
          for {
            v <- txnVar.get
          } yield (this, v)
      }

    override private[stm] lazy val isDirty: F[Boolean] =
      validLog.isDirty

    override private[stm] lazy val getRetrySignal
        : F[Option[Deferred[F, Unit]]] =
      for {
        retrySignal <- Deferred[F, Unit]
        registerRetries <-
          validLog.log.values.toList.traverse(_.getRegisterRetry)
        _ <- registerRetries.traverse(rr => rr(retrySignal))
      } yield Some(retrySignal)

    override private[stm] lazy val scheduleRetry =
      Async[F].pure(this)

    override private[stm] lazy val commit: F[Unit] =
      Async[F].unit

    override private[stm] lazy val idClosure: F[IdClosure] =
      Async[F].pure(IdClosure.empty)
  }

  private[stm] case class TxnLogError(ex: Throwable) extends TxnLog {

    override private[stm] def getVar[V](
        txnVar: TxnVar[F, V]
    ): F[(TxnLog, V)] =
      txnVar.get.map(v => (this, v))

    override private[stm] lazy val scheduleRetry =
      Async[F].pure(this)

    override private[stm] lazy val isDirty =
      Async[F].pure(false)

    override private[stm] lazy val commit: F[Unit] =
      Async[F].unit

    override private[stm] lazy val idClosure: F[IdClosure] =
      Async[F].pure(IdClosure.empty)

    override private[stm] lazy val getRetrySignal
        : F[Option[Deferred[F, Unit]]] =
      Async[F].pure(None)
  }

  private[stm] case class TxnRetryException(validLog: TxnLogValid)
      extends RuntimeException

}
