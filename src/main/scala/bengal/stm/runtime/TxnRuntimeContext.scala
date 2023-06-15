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

import bengal.stm.model.runtime._
import bengal.stm.model._

import cats.effect.implicits._
import cats.effect.kernel.Async
import cats.effect.std.Semaphore
import cats.effect.{Deferred, Ref}
import cats.syntax.all._

import scala.collection.mutable.{ListBuffer, Map => MutableMap}
import scala.concurrent.duration.FiniteDuration

private[stm] trait TxnRuntimeContext[F[_]] {
  this: AsyncImplicits[F]
    with TxnCompilerContext[F]
    with TxnLogContext[F]
    with TxnAdtContext[F] =>

  private[stm] val txnIdGen: Ref[F, TxnId]
  private[stm] val txnVarIdGen: Ref[F, TxnVarId]

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

    private def withLock[A](semaphore: Semaphore[F])(fa: F[A]): F[A] =
      semaphore.permit.use(_ => fa)

    private[stm] def triggerReprocessing: F[Unit] =
      schedulerTrigger.get.flatMap(_.complete(())).void

    private[stm] def registerCompletion(
        txnId: TxnId,
        idClosure: IdClosure
    ): F[Unit] =
      for {
        closureFib <-
          Async[F].delay(closureTallies.removeIdClosure(idClosure)).start
        _                     <- waitingSemaphore.acquire
        _                     <- withLock(runningSemaphore)(Async[F].delay(runningMap -= txnId))
        waitingBufferNonEmpty <- Async[F].delay(waitingBuffer.nonEmpty)
        _ <- if (waitingBufferNonEmpty)
               closureFib.joinWithNever >> triggerReprocessing
             else Async[F].unit
        _ <- waitingSemaphore.release
      } yield ()

    private[stm] def submitTxn[V](
        analysedTxn: AnalysedTxn[V]
    ): F[Unit] =
      for {
        _ <- waitingSemaphore.acquire
        _ <- Async[F].delay(waitingBuffer.append(analysedTxn))
        _ <- Async[F].delay(closureTallies.addIdClosure(analysedTxn.idClosure))
        _ <- triggerReprocessing
        _ <- waitingSemaphore.release
      } yield ()

    private[stm] def submitTxnForImmediateRetry[V](
        analysedTxn: AnalysedTxn[V]
    ): F[Unit] =
      for {
        _ <- waitingSemaphore.acquire
        _ <- Async[F].delay(waitingBuffer.prepend(analysedTxn))
        _ <- Async[F].delay(closureTallies.addIdClosure(analysedTxn.idClosure))
        _ <- triggerReprocessing
        _ <- waitingSemaphore.release
      } yield ()

    private def attemptExecution(
        analysedTxn: AnalysedTxn[_]
    ): F[Unit] =
      for {
        _ <- withLock(runningSemaphore) {
               Async[F].delay(runningMap += (analysedTxn.id -> analysedTxn))
             }
        _ <- analysedTxn.execute(this).start
      } yield ()

    private def getRunningClosure: F[IdClosure] =
      for {
        _ <- runningSemaphore.acquire
        result <- Async[F].ifM(Async[F].delay(runningMap.nonEmpty))(
                    for {
                      innerResult <- Async[F].delay {
                                       runningMap.values
                                         .map(_.idClosure)
                                         .reduce(_ mergeWith _)
                                     }
                      _ <- runningSemaphore.release
                    } yield innerResult,
                    runningSemaphore.release.map(_ => IdClosure.empty)
                  )
      } yield result

    private def idClosureAnalysisRecursion(
        stillWaiting: List[AnalysedTxn[_]],
        cycles: Int
    ): F[Unit] = {
      val newWaiting: F[(Boolean, List[AnalysedTxn[_]], Int)] =
        for {
          currentClosure <- getRunningClosure
          aTxn           <- Async[F].delay(waitingBuffer.remove(0))
          (newStillWaiting, newClosure, newCycles) <-
            Async[F].ifM(
              Async[F].delay(aTxn.idClosure.isCompatibleWith(currentClosure))
            )(
              for {
                _ <-
                  attemptExecution(aTxn)
                newClosure <-
                  Async[F].delay(currentClosure.mergeWith(aTxn.idClosure))
              } yield (stillWaiting, newClosure, cycles),
              for {
                newStillWaiting <-
                  Async[F].delay(aTxn :: stillWaiting)
              } yield (newStillWaiting, currentClosure, cycles - 1)
            )
          attemptReprocess <-
            Async[F].ifM(Async[F].delay(waitingBuffer.isEmpty))(
              Async[F].pure(false),
              Async[F].ifM(Async[F].delay(waitingBuffer.isEmpty))(
                Async[F].pure(false),
                Async[F].ifM(Async[F].delay(newCycles == -1))(
                  Async[F].pure(true),
                  Async[F].ifM(
                    Async[F].delay(newCycles > maxWaitingToProcessInLoop)
                  )(
                    Async[F].pure(false),
                    Async[F].delay(
                      closureTallies.getIdClosure.mergeWith(
                        currentClosure
                      ) != newClosure
                    )
                  )
                )
              )
            )
        } yield (attemptReprocess, newStillWaiting, newCycles + 1)

      newWaiting.flatMap {
        case (attemptReprocess, newStillWaiting, newCycles) =>
          Async[F].ifM(Async[F].pure(attemptReprocess))(
            idClosureAnalysisRecursion(newStillWaiting, newCycles),
            idClosureAnalysisTerminate(newStillWaiting)
          )
      }
    }

    private def idClosureAnalysisTerminate(
        stillWaiting: List[AnalysedTxn[_]]
    ): F[Unit] =
      Async[F].delay(stillWaiting.foreach(waitingBuffer.prepend))

    private[stm] def reprocessWaiting: F[Unit] =
      for {
        newTrigger <- Deferred[F, Unit]
        _          <- schedulerTrigger.get.flatMap(_.get)
        _          <- waitingSemaphore.acquire
        _          <- schedulerTrigger.set(newTrigger)
        _ <- Async[F].ifM(Async[F].delay(waitingBuffer.nonEmpty))(
               idClosureAnalysisRecursion(List(), 0),
               idClosureAnalysisTerminate(List())
             )
        _ <- waitingSemaphore.release
      } yield ()

    private[stm] def reprocessingRecursion: F[Unit] =
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

    private[stm] def getTxnLogResult: F[(TxnLog, Option[V])] =
      txn
        .foldMap[TxnLogStore](txnLogCompiler)
        .run(TxnLogValid.empty)
        .map { res =>
          (res._1, Option(res._2))
        }
        .handleErrorWith {
          case TxnRetryException(log) =>
            Async[F].delay((TxnLogRetry(log), None))
          case ex =>
            Async[F].delay((TxnLogError(ex), None))
        }

    private val commit: F[TxnResult] =
      for {
        logResult <- getTxnLogResult
        (log, logValue) = logResult
        result <- log match {
                    case TxnLogValid(_) =>
                      Async[F].uncancelable { _ =>
                        log.withLock {
                          Async[F].ifM[Option[V]](log.isDirty)(
                            Async[F].delay(None),
                            log.commit.as(logValue)
                          )
                        }
                      }.flatMap { s =>
                        s.map(v =>
                          Async[F]
                            .delay(TxnResultSuccess(v).asInstanceOf[TxnResult])
                        ).getOrElse(
                          log.idClosure
                            .map(TxnResultLogDirty(_).asInstanceOf[TxnResult])
                        )
                      }
                    case retry @ TxnLogRetry(_) =>
                      Async[F].uncancelable { _ =>
                        retry.validLog.withLock {
                          Async[F].ifM[TxnResult](log.isDirty)(
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
                      Async[F].delay(TxnResultFailure(err))
                  }
      } yield result

    private[stm] def execute(
        ex: TxnScheduler
    ): F[Unit] =
      Async[F].uncancelable { poll =>
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
                       _ <-
                         Async[F].race(retrySignal.get,
                                       Async[F].sleep(ex.retryWaitMaxDuration)
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

    private[stm] def commit[V](txn: Txn[V]): F[V] =
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
                Async[F].delay((idClosure, None))
              case _ =>
                Async[F].pure((IdClosure.empty, None))
            }
        completionSignal <- Deferred[F, Either[Throwable, V]]
        id               <- txnIdGen.getAndUpdate(_ + 1)
        analysedTxn <-
          Async[F].delay(
            AnalysedTxn(id, txn, staticAnalysisResult._1, completionSignal)
          )
        _          <- scheduler.submitTxn(analysedTxn).start
        completion <- completionSignal.get
        result <- completion match {
                    case Right(res) => Async[F].pure(res)
                    case Left(ex)   => Async[F].raiseError(ex)
                  }
      } yield result
  }
}
