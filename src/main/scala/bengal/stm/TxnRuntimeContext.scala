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

import bengal.stm.TxnRuntimeContext.{IdClosure, IdClosureTallies}
import bengal.stm.TxnStateEntityContext.{TxnId, TxnVarRuntimeId}

import cats.effect.implicits._
import cats.effect.kernel.{Concurrent, Temporal}
import cats.effect.std.Semaphore
import cats.effect.{Deferred, Ref}
import cats.implicits._

import scala.annotation.nowarn
import scala.collection.mutable.{ListBuffer, Map => MutableMap}
import scala.collection.concurrent.{TrieMap, Map => ConcurrentMap}
import scala.concurrent.duration.FiniteDuration

private[stm] trait TxnRuntimeContext[F[_]] {
  this: TxnAdtContext[F] with TxnCompilerContext[F] with TxnLogContext[F] =>

  private[stm] val txnIdGen: Ref[F, TxnId]

  private[stm] sealed trait TxnResult
  private[stm] case class TxnResultSuccess[V](result: V) extends TxnResult

  private[stm] case class TxnResultRetry(retrySignal: Deferred[F, Unit])
      extends TxnResult

  private[stm] case class TxnResultLogDirty(idClosureRefinement: IdClosure)
      extends TxnResult

  private[stm] case class TxnResultFailure(ex: Throwable) extends TxnResult

  private[stm] object TxnResultFailure {

    private[stm] def apply(logFailure: TxnLogError): TxnResultFailure =
      TxnResultFailure(logFailure.ex)
  }

  private[stm] case class TxnScheduler(
      runningMap: MutableMap[TxnId, AnalysedTxn[_]],
      runningSemaphore: Semaphore[F],
      waitingBuffer: ListBuffer[AnalysedTxn[_]],
      waitingSemaphore: Semaphore[F],
      closureTallies: IdClosureTallies,
      schedulerTrigger: Ref[F, Deferred[F, Unit]],
      retryWaitMaxDuration: FiniteDuration,
      maxWaitingToProcessInLoop: Int
  ) {

    private def withLock[A](semaphore: Semaphore[F])(fa: F[A])(implicit
        F: Concurrent[F]
    ): F[A] =
      semaphore.permit.use(_ => fa)

    private[stm] def triggerReprocessing(implicit F: Concurrent[F]): F[Unit] =
      schedulerTrigger.get.flatMap(_.complete(())).void

    private[stm] def registerCompletion(
        txnId: TxnId,
        idClosure: IdClosure
    )(implicit F: Concurrent[F]): F[Unit] =
      for {
        closureFib <- F.pure(closureTallies.removeIdClosure(idClosure)).start
        _          <- waitingSemaphore.acquire
        _          <- withLock(runningSemaphore)(F.pure(runningMap -= txnId))
        _ <- if (waitingBuffer.nonEmpty)
               closureFib.joinWithNever >> triggerReprocessing
             else F.unit
        _ <- waitingSemaphore.release
      } yield ()

    private[stm] def submitTxn[V](
        analysedTxn: AnalysedTxn[V]
    )(implicit F: Concurrent[F]): F[Unit] =
      for {
        _ <- waitingSemaphore.acquire
        _ <- F.pure(waitingBuffer.append(analysedTxn))
        _ <- F.pure(closureTallies.addIdClosure(analysedTxn.idClosure))
        _ <- triggerReprocessing
        _ <- waitingSemaphore.release
      } yield ()

    private[stm] def submitTxnForImmediateRetry[V](
        analysedTxn: AnalysedTxn[V]
    )(implicit F: Concurrent[F]): F[Unit] =
      for {
        _ <- waitingSemaphore.acquire
        _ <- F.pure(waitingBuffer.prepend(analysedTxn))
        _ <- F.pure(closureTallies.addIdClosure(analysedTxn.idClosure))
        _ <- triggerReprocessing
        _ <- waitingSemaphore.release
      } yield ()

    private def attemptExecution(
        analysedTxn: AnalysedTxn[_]
    )(implicit F: Concurrent[F], TF: Temporal[F]): F[Unit] =
      for {
        _ <- withLock(runningSemaphore) {
               F.pure(runningMap += (analysedTxn.id -> analysedTxn))
             }
        _ <- analysedTxn.execute(this).start
      } yield ()

    private def getRunningClosure(implicit F: Concurrent[F]): F[IdClosure] =
      for {
        _ <- runningSemaphore.acquire
        result <- if (runningMap.nonEmpty) {
                    for {
                      innerResult <- F.pure {
                                       runningMap.values
                                         .map(_.idClosure)
                                         .reduce(_ mergeWith _)
                                     }
                      _ <- runningSemaphore.release
                    } yield innerResult
                  } else {
                    runningSemaphore.release.map(_ => IdClosure.empty)
                  }
      } yield result

    private def idClosureAnalysisRecursion(
        currentClosure: IdClosure,
        finalClosure: IdClosure,
        stillWaiting: List[AnalysedTxn[_]] = List(),
        cycles: Int = 0
    )(implicit F: Concurrent[F], TF: Temporal[F]): F[Unit] =
      if (
        (currentClosure != finalClosure && cycles <= maxWaitingToProcessInLoop) || cycles == 0
      ) {
        val newWaiting: F[(IdClosure, List[AnalysedTxn[_]])] = for {
          aTxn <- F.pure(waitingBuffer.head)
          _    <- F.pure(waitingBuffer.dropInPlace(1))
          result <- if (aTxn.idClosure.isCompatibleWith(currentClosure)) {
                      attemptExecution(aTxn) >> F.pure(stillWaiting)
                    } else {
                      F.pure(aTxn :: stillWaiting)
                    }
        } yield (aTxn.idClosure, result)

        newWaiting.flatMap { nw =>
          idClosureAnalysisRecursion(currentClosure.mergeWith(nw._1),
                                     finalClosure,
                                     nw._2,
                                     cycles + 1
          )
        }
      } else {
        F.pure(stillWaiting.foreach(waitingBuffer.prepend))
      }

    private[stm] def reprocessWaiting(implicit
        F: Concurrent[F],
        TF: Temporal[F]
    ): F[Unit] =
      for {
        newTrigger <- Deferred[F, Unit]
        _          <- schedulerTrigger.get.flatMap(_.get)
        _          <- waitingSemaphore.acquire
        _          <- schedulerTrigger.set(newTrigger)
        _ <- for {
               runningClosure <- getRunningClosure
               currentClosure <- F.pure(closureTallies.getIdClosure)
               totalClosure = currentClosure.mergeWith(runningClosure)
               _ <- idClosureAnalysisRecursion(
                      runningClosure,
                      totalClosure
                    )
             } yield ()
        _ <- waitingSemaphore.release
      } yield ()

    @nowarn
    private[stm] def reprocessingRecursion(implicit
        F: Concurrent[F],
        TF: Temporal[F]
    ): F[Unit] =
      reprocessWaiting.flatMap(_ => reprocessingRecursion)
  }

  private[stm] object TxnScheduler {

    private[stm] def apply(
        runningSemaphore: Semaphore[F],
        waitingSemaphore: Semaphore[F],
        schedulerTrigger: Ref[F, Deferred[F, Unit]],
        retryWaitMaxDuration: FiniteDuration,
        maxWaitingToProcessInLoop: Int
    ): TxnScheduler =
      TxnScheduler(
        runningMap = MutableMap(),
        runningSemaphore = runningSemaphore,
        waitingBuffer = ListBuffer(),
        waitingSemaphore = waitingSemaphore,
        closureTallies = IdClosureTallies.empty,
        schedulerTrigger = schedulerTrigger,
        retryWaitMaxDuration = retryWaitMaxDuration,
        maxWaitingToProcessInLoop = maxWaitingToProcessInLoop
      )

  }

  private[stm] case class AnalysedTxn[V](
      id: TxnId,
      txn: Txn[V],
      idClosure: IdClosure,
      completionSignal: Deferred[F, Either[Throwable, V]]
  ) {

    private[stm] def getTxnLogResult(implicit
        F: Concurrent[F]
    ): F[(TxnLog, Option[V])] =
      txn
        .foldMap[TxnLogStore](txnLogCompiler)
        .run(TxnLogValid.empty)
        .map { res =>
          (res._1, Option(res._2))
        }
        .handleErrorWith {
          case TxnRetryException(log) =>
            F.pure((TxnLogRetry(log), None))
          case ex =>
            F.pure((TxnLogError(ex), None))
        }

    private def commit(implicit F: Concurrent[F]): F[TxnResult] =
      for {
        logResult <- getTxnLogResult
        (log, logValue) = logResult
        result <- log match {
                    case TxnLogValid(_) =>
                      F.uncancelable { _ =>
                        log.withLock {
                          F.ifM[Option[V]](log.isDirty)(
                            F.pure(None),
                            log.commit.as(logValue)
                          )
                        }
                      }.flatMap { s =>
                        s.map(v =>
                          F.pure(TxnResultSuccess(v).asInstanceOf[TxnResult])
                        ).getOrElse(
                          log.idClosure
                            .map(TxnResultLogDirty(_).asInstanceOf[TxnResult])
                        )
                      }
                    case retry @ TxnLogRetry(_) =>
                      F.uncancelable { _ =>
                        retry.validLog.withLock {
                          F.ifM[TxnResult](log.isDirty)(
                            retry.validLog.idClosure.map(
                              TxnResultLogDirty(_).asInstanceOf[TxnResult]
                            ),
                            retry.getRetrySignal
                              .map(_.map(TxnResultRetry).getOrElse {
                                TxnResultFailure(
                                  new RuntimeException(
                                    "No retry signal present for transaction!"
                                  )
                                )
                              })
                              .map(_.asInstanceOf[TxnResult])
                          )
                        }
                      }
                    case err @ TxnLogError(_) =>
                      F.pure(TxnResultFailure(err))
                  }
      } yield result

    private[stm] def execute(
        ex: TxnScheduler
    )(implicit F: Concurrent[F], TF: Temporal[F]): F[Unit] =
      F.uncancelable { poll =>
        for {
          result <- poll(commit)
          _      <- ex.registerCompletion(id, idClosure)
          _ <- result match {
                 case TxnResultSuccess(result) =>
                   completionSignal.complete(
                     Right[Throwable, V](result.asInstanceOf[V])
                   )
                 case TxnResultRetry(retrySignal) =>
                   poll {
                     for {
                       _ <- F.race(retrySignal.get,
                                   TF.sleep(ex.retryWaitMaxDuration)
                            )
                       _ <- ex.submitTxn(this)
                     } yield ()
                   }
                 case TxnResultLogDirty(idClosureRefinement) =>
                   poll {
                     ex.submitTxnForImmediateRetry(
                       this.copy(idClosure = idClosureRefinement)
                     )
                   }
                 case TxnResultFailure(err) =>
                   completionSignal.complete(Left[Throwable, V](err))
               }
        } yield ()
      }
  }

  private[stm] trait TxnRuntime {

    private[stm] val scheduler: TxnScheduler

    private[stm] def commit[V](txn: Txn[V])(implicit F: Concurrent[F]): F[V] =
      for {
        staticAnalysisResult <-
          txn
            .foldMap[IdClosureStore](staticAnalysisCompiler)
            .run(IdClosure.empty)
            .map { res =>
              (res._1, Option(res._2))
            }
            // Sometimes the static analysis will unavoidably throw
            // due to impossible casts being attempted. In this case, we
            // leverage the closure gathered to the point in the free recursion
            // up to where the error is generated. In the worst case,
            // we fall back to blindly optimistic scheduling (for the
            // first transaction attempt)
            .handleErrorWith {
              case StaticAnalysisShortCircuitException(idClosure) =>
                F.pure((idClosure, None))
              case _ =>
                F.pure((IdClosure.empty, None))
            }
        completionSignal <- Deferred[F, Either[Throwable, V]]
        id               <- txnIdGen.getAndUpdate(_ + 1)
        analysedTxn <-
          F.pure(
            AnalysedTxn(id, txn, staticAnalysisResult._1, completionSignal)
          )
        _          <- scheduler.submitTxn(analysedTxn).start
        completion <- completionSignal.get
        result <- completion match {
                    case Right(res) => F.pure(res)
                    case Left(ex)   => F.raiseError(ex)
                  }
      } yield result
  }
}

private[stm] object TxnRuntimeContext {

  private[stm] case class IdClosure(
      readIds: Set[TxnVarRuntimeId],
      updatedIds: Set[TxnVarRuntimeId]
  ) {

    private[stm] lazy val combinedIds: Set[TxnVarRuntimeId] =
      readIds ++ updatedIds

    private[stm] def addReadId(id: TxnVarRuntimeId): IdClosure =
      this.copy(readIds = readIds + id)

    private[stm] def addWriteId(id: TxnVarRuntimeId): IdClosure =
      this.copy(updatedIds = updatedIds + id)

    private[stm] def mergeWith(idScope: IdClosure): IdClosure =
      this.copy(readIds = readIds ++ idScope.readIds,
                updatedIds = updatedIds ++ idScope.updatedIds
      )

    private[stm] def isCompatibleWith(idScope: IdClosure): Boolean =
      combinedIds.intersect(idScope.updatedIds).isEmpty && idScope.combinedIds
        .intersect(updatedIds)
        .isEmpty
  }

  private[stm] object IdClosure {
    private[stm] val empty: IdClosure = IdClosure(Set(), Set())
  }

  private[stm] case class IdClosureTallies(
      private val readIdTallies: ConcurrentMap[TxnVarRuntimeId, Int],
      private val updatedIdTallies: ConcurrentMap[TxnVarRuntimeId, Int]
  ) {

    private def addReadId(id: TxnVarRuntimeId): Unit =
      readIdTallies += (id -> (readIdTallies.getOrElse(id, 0) + 1))

    private def removeReadId(id: TxnVarRuntimeId): Unit = {
      val newValue: Int = readIdTallies.getOrElse(id, 0) - 1
      if (newValue < 1) {
        readIdTallies -= id
      } else {
        readIdTallies += (id -> newValue)
      }
    }

    private def addUpdateId(id: TxnVarRuntimeId): Unit =
      updatedIdTallies += (id -> (updatedIdTallies.getOrElse(id, 0) + 1))

    private def removeUpdateId(id: TxnVarRuntimeId): Unit = {
      val newValue: Int = updatedIdTallies.getOrElse(id, 0) - 1
      if (newValue < 1) {
        updatedIdTallies -= id
      } else {
        updatedIdTallies += (id -> newValue)
      }
    }

    private def addReadIds(ids: Set[TxnVarRuntimeId]): Unit =
      ids.foreach(addReadId)

    private def removeReadIds(ids: Set[TxnVarRuntimeId]): Unit =
      ids.foreach(removeReadId)

    private def addUpdateIds(ids: Set[TxnVarRuntimeId]): Unit =
      ids.foreach(addUpdateId)

    private def removeUpdateIds(ids: Set[TxnVarRuntimeId]): Unit =
      ids.foreach(removeUpdateId)

    private[stm] def addIdClosure(idClosure: IdClosure): Unit = {
      addReadIds(idClosure.readIds)
      addUpdateIds(idClosure.updatedIds)
    }

    private[stm] def removeIdClosure(idClosure: IdClosure): Unit = {
      removeReadIds(idClosure.readIds)
      removeUpdateIds(idClosure.updatedIds)
    }

    private[stm] def getIdClosure: IdClosure =
      IdClosure(
        readIds = readIdTallies.keySet.toSet,
        updatedIds = updatedIdTallies.keySet.toSet
      )
  }

  private[stm] object IdClosureTallies {

    private[stm] def empty: IdClosureTallies =
      IdClosureTallies(TrieMap.empty, TrieMap.empty)
  }
}
