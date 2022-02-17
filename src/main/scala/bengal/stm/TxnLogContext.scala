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
    private[stm] def lock(implicit F: Concurrent[F]): F[Semaphore[F]]
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
      TxnLogUpdateVarEntry[V](
        initial = initial,
        current = value,
        txnVar = txnVar
      )

    override private[stm] def commit(implicit F: Concurrent[F]): F[Unit] =
      F.unit

    override private[stm] def isDirty(implicit F: Concurrent[F]): F[Boolean] =
      txnVar.get.map(_ != initial)

    override private[stm] def lock(implicit F: Concurrent[F]): F[Semaphore[F]] =
      F.pure(txnVar.commitLock)

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
      TxnLogUpdateVarEntry[V](
        initial = initial,
        current = value,
        txnVar = txnVar
      )

    override private[stm] def commit(implicit F: Concurrent[F]): F[Unit] =
      txnVar.set(current)

    override private[stm] def isDirty(implicit F: Concurrent[F]): F[Boolean] =
      txnVar.get.map(_ != initial)

    override private[stm] def lock(implicit F: Concurrent[F]): F[Semaphore[F]] =
      F.pure(txnVar.commitLock)

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
  private[stm] case class TxnLogReadOnlyVarMapEntry[K, V](
      key: K,
      initial: Option[V],
      txnVarMap: TxnVarMap[K, V]
  ) extends TxnLogEntry[Option[V]] {

    override private[stm] def get: Option[V] =
      initial

    override private[stm] def set(value: Option[V]): TxnLogEntry[Option[V]] =
      TxnLogUpdateVarMapEntry(
        key = key,
        initial = initial,
        current = value,
        txnVarMap = txnVarMap
      )

    override private[stm] def commit(implicit F: Concurrent[F]): F[Unit] =
      F.unit

    override private[stm] def isDirty(implicit F: Concurrent[F]): F[Boolean] =
      txnVarMap.get(key).map { oValue =>
        initial
          .map(iValue => !oValue.contains(iValue))
          .getOrElse(oValue.isDefined)
      }

    override private[stm] def lock(implicit F: Concurrent[F]): F[Semaphore[F]] =
      for {
        oTxnVar <- txnVarMap.getTxnVar(key)
        result <- oTxnVar match {
                    case Some(txnVar) =>
                      F.pure(txnVar.commitLock)
                    case None =>
                      Semaphore[F](1)
                  }
      } yield result

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
      txnVarMap.getRuntimeId(key).map { rid =>
        IdClosure(
          readIds = Set(rid),
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
      TxnLogUpdateVarMapEntry(
        key = key,
        initial = initial,
        current = value,
        txnVarMap = txnVarMap
      )

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

    override private[stm] def lock(implicit F: Concurrent[F]): F[Semaphore[F]] =
      for {
        oTxnVar <- txnVarMap.getTxnVar(key)
        result <- oTxnVar match {
                    case Some(txnVar) =>
                      F.pure(txnVar.commitLock)
                    case None =>
                      Semaphore[F](1)
                  }
      } yield result

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
      txnVarMap.getRuntimeId(key).map { rid =>
        IdClosure(
          readIds = Set(rid),
          updatedIds = Set(rid)
        )
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
    private[stm] def getCurrentValue[K, V](
        txnVarMap: TxnVarMap[K, V]
    ): Map[K, V] =
      Map()

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

    override private[stm] def getCurrentValue[K, V](
        key: K,
        txnVarMap: TxnVarMap[K, V]
    )(implicit
        F: Concurrent[F]
    ): F[Option[V]] =
      for {
        rid <- txnVarMap.getRuntimeId(key)
      } yield log.get(rid) match {
        case Some(entry) => entry.get.asInstanceOf[Option[V]]
        case None        => None
      }

    override private[stm] def getCurrentValue[K, V](
        txnVarMap: TxnVarMap[K, V]
    ): Map[K, V] =
      log.values.flatMap {
        case TxnLogReadOnlyVarMapEntry(key, Some(initial), entryMap)
            if txnVarMap.id == entryMap.id =>
          Some(key -> initial)
        case TxnLogUpdateVarMapEntry(key, _, Some(current), entryMap)
            if txnVarMap.id == entryMap.id =>
          Some(key -> current)
        case _ => None
      }.toMap.asInstanceOf[Map[K, V]]

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
                        rid <- txnVarMap.getRuntimeId(key)
                      } yield log.get(rid) match {
                        case Some(_) =>
                          this //Noop
                        case None =>
                          TxnLogError {
                            new RuntimeException(
                              s"Tried to read non-existent key $key in transactional map"
                            )
                          }
                      }
                  }
      } yield result

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
                  this.copy(
                    log + (txnVar.runtimeId -> TxnLogUpdateVarMapEntry(
                      key,
                      Some(v),
                      Some(newValue),
                      txnVarMap
                    ))
                  )
                }
            }
          case None =>
            // The txnVar may have been set to be created in this transaction
            txnVarMap.getRuntimeId(key).map { rid =>
              log.get(rid) match {
                case Some(entry) =>
                  this.copy(
                    log + (rid -> entry
                      .asInstanceOf[TxnLogEntry[Option[V]]]
                      .set(Some(newValue)))
                  )
                case None =>
                  this.copy(
                    log + (rid -> TxnLogUpdateVarMapEntry(key,
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
              .map { rid =>
                log.get(rid) match {
                  case Some(entry) =>
                    entry.get.asInstanceOf[Option[V]] match {
                      case Some(v) =>
                        this.copy(
                          log + (rid -> entry
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
                  case None =>
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
                        rid <- txnVarMap.getRuntimeId(key)
                      } yield log.get(rid) match {
                        case Some(entry) =>
                          this.copy(
                            log + (rid -> entry
                              .asInstanceOf[TxnLogEntry[Option[V]]]
                              .set(None))
                          )
                        case None => // Throw error to be consistent with read behaviour
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
      for {
        registerRetries <- log.values.toList.parTraverse(_.getRegisterRetry)
      } yield TxnLogRetry(signal =>
        registerRetries.parTraverse(rr => rr(signal)).void
      )

    override private[stm] def isDirty(implicit F: Concurrent[F]): F[Boolean] =
      log.values.toList.parTraverse { entry =>
        entry.isDirty
      }.map(_.exists(isdi => isdi))

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
        result <- locks
                    .foldLeft(Resource.eval(F.unit))((i, j) => i >> j.permit)
                    .use(_ => fa)
      } yield result

    override private[stm] def commit(implicit F: Concurrent[F]): F[Unit] =
      log.values.toList.parTraverse(_.commit).void
  }

  private[stm] object TxnLogValid {
    private[stm] val empty: TxnLogValid = TxnLogValid(Map())
  }

  private[stm] case class TxnLogRetry(
      registerRetry: Deferred[F, Unit] => F[Unit]
  ) extends TxnLog {

    override private[stm] def raiseError(ex: Throwable)(implicit
        F: Concurrent[F]
    ): F[TxnLog] =
      F.pure(TxnLogError(ex))
  }

  private[stm] case class TxnLogError(ex: Throwable) extends TxnLog
}
