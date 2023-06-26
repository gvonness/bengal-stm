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

import scala.collection.mutable.{Map => MutableMap}
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

  private[stm] case object TxnResultRetry extends TxnResult

  private[stm] case class TxnResultLogDirty(idClosureRefinement: IdClosure)
      extends TxnResult

  private[stm] case class TxnResultFailure(ex: Throwable) extends TxnResult

  private[stm] object TxnResultFailure {

    private[stm] def apply(logFailure: TxnLogError): TxnResultFailure =
      TxnResultFailure(logFailure.ex)
  }

  private[stm] case class TxnScheduler(
      graphBuilderSemaphore: Semaphore[F],
      activeTransactions: MutableMap[TxnId, AnalysedTxn[_]],
      retryWaitMaxDuration: FiniteDuration
  ) {

    def submitTxnForImmediateRetry(analysedTxn: AnalysedTxn[_]): F[Unit] =
      for {
        _ <- analysedTxn.resetDependencyTally
        _ <- graphBuilderSemaphore.acquire
        testAndLink <- activeTransactions.values.toList.parTraverse { aTxn =>
          (for {
                 status <- aTxn.executionStatus.get
                 _ <- status match {
                        case Running =>
                          Async[F].ifM(
                            Async[F].delay(
                              analysedTxn.idClosure.isCompatibleWith(
                                aTxn.idClosure
                              )
                            )
                          )(
                            Async[F].unit,
                            aTxn.subscribeDownstreamDependency(analysedTxn,
                                                               this
                            )
                          )
                        case Scheduled =>
                          Async[F].ifM(
                            Async[F].delay(
                              analysedTxn.idClosure.isCompatibleWith(
                                aTxn.idClosure
                              )
                            )
                          )(
                            Async[F].unit,
                            analysedTxn.subscribeDownstreamDependency(aTxn,
                                                                      this
                            )
                          )
                        case _ =>
                          Async[F].unit
                      }
               } yield ()).start
             }
        _ <- analysedTxn.executionStatus.set(Scheduled)
        _ <-
          Async[F].delay(
            activeTransactions.addOne(analysedTxn.id -> analysedTxn)
          )
        _ <- testAndLink.parTraverse(_.joinWithNever)
        _ <- analysedTxn.checkExecutionReadiness(this)
        _ <- graphBuilderSemaphore.release
      } yield ()

    def submitTxn(analysedTxn: AnalysedTxn[_]): F[Unit] =
      for {
        _ <- analysedTxn.resetDependencyTally
        _ <- graphBuilderSemaphore.acquire
        testAndLink <- activeTransactions.values.toList.parTraverse { aTxn =>
                         Async[F]
                           .ifM(
                             Async[F].delay(
                               analysedTxn.idClosure.isCompatibleWith(
                                 aTxn.idClosure
                               )
                             )
                           )(Async[F].unit,
                             aTxn.subscribeDownstreamDependency(analysedTxn,
                                                                this
                             )
                           )
                           .start
                       }
        _ <- analysedTxn.executionStatus.set(Scheduled)
        _ <-
          Async[F].delay(
            activeTransactions.addOne(analysedTxn.id -> analysedTxn)
          )
        _ <- testAndLink.parTraverse(_.joinWithNever)
        _ <- analysedTxn.checkExecutionReadiness(this)
        _ <- graphBuilderSemaphore.release
      } yield ()

    def registerCompletion(analysedTxn: AnalysedTxn[_]): F[Unit] =
      for {
        _ <- graphBuilderSemaphore.acquire
        _ <- Async[F].delay(activeTransactions.remove(analysedTxn.id))
        _ <- graphBuilderSemaphore.release
        _ <- analysedTxn.triggerUnsub.start
      } yield ()

    def registerRunning(analysedTxn: AnalysedTxn[_]): F[Unit] =
      for {
        _ <- graphBuilderSemaphore.acquire
        _ <- Async[F].delay(analysedTxn.executionStatus.set(Running))
        _ <- graphBuilderSemaphore.release
      } yield ()
  }

  private[stm] object TxnScheduler {

    private[stm] def apply(
        graphBuilderSemaphore: Semaphore[F],
        retryWaitMaxDuration: FiniteDuration
    ): TxnScheduler =
      TxnScheduler(
        activeTransactions = MutableMap(),
        graphBuilderSemaphore = graphBuilderSemaphore,
        retryWaitMaxDuration = retryWaitMaxDuration
      )

  }

  private[stm] case class AnalysedTxn[V](
      id: TxnId,
      txn: Txn[V],
      idClosure: IdClosure,
      completionSignal: Deferred[F, Either[Throwable, V]],
      dependencyTally: Ref[F, Int],
      unsubSpecs: MutableMap[TxnId, F[Unit]],
      executionStatus: Ref[F, ExecutionStatus]
  ) {

    private[stm] val resetDependencyTally: F[Unit] =
      dependencyTally.set(0)

    private[stm] def checkExecutionReadiness(scheduler: TxnScheduler): F[Unit] =
      Async[F].ifM(dependencyTally.get.map(_ == 0))(
        execute(scheduler),
        Async[F].unit
      )

    private def unsubscribeUpstreamDependency(
        scheduler: TxnScheduler
    ): F[Unit] =
      Async[F].ifM(dependencyTally.getAndUpdate(_ - 1).map(_ == 1))(
        execute(scheduler),
        Async[F].unit
      )

    private val subscribeUpstreamDependency: F[Unit] =
      dependencyTally.update(_ + 1)

    private[stm] def subscribeDownstreamDependency(
        txn: AnalysedTxn[_],
        scheduler: TxnScheduler
    ): F[Unit] =
      Async[F].ifM(Async[F].delay(unsubSpecs.keys.toSet.contains(txn.id)))(
        Async[F].unit,
        for {
          _ <- txn.subscribeUpstreamDependency
          _ <-
            Async[F]
              .delay(
                unsubSpecs.addOne(
                  txn.id -> txn.unsubscribeUpstreamDependency(scheduler)
                )
              )
        } yield ()
      )

    private[stm] val triggerUnsub: F[Unit] =
      Async[F].ifM(Async[F].delay(unsubSpecs.nonEmpty))(
        for {
          _ <- unsubSpecs.values.toList.parTraverse(unsubSpec => unsubSpec)
          _ <- Async[F].delay(unsubSpecs.clear())
        } yield (),
        Async[F].unit
      )

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
                            .map(closure =>
                              TxnResultLogDirty(closure).asInstanceOf[TxnResult]
                            )
                        )
                      }
                    case retry @ TxnLogRetry(_) =>
                      Async[F].uncancelable { _ =>
                        retry.validLog.withLock {
                          Async[F].ifM[TxnResult](log.isDirty)(
                            retry.validLog.idClosure.map(closure =>
                              TxnResultLogDirty(closure).asInstanceOf[TxnResult]
                            ),
                            Async[F].delay(
                              TxnResultRetry.asInstanceOf[TxnResult]
                            )
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
      for {
        _ <- ex.registerRunning(this)
        _ <- Async[F].uncancelable { poll =>
               for {
                 result <- poll(commit)
                 _      <- ex.registerCompletion(this)
                 _ <- result match {
                        case TxnResultSuccess(result) =>
                          completionSignal.complete(
                            Right[Throwable, V](result.asInstanceOf[V])
                          )
                        case TxnResultRetry =>
                          ex.submitTxn(this)
                        case TxnResultLogDirty(idClosureRefinement) =>
                          ex.submitTxnForImmediateRetry(
                            this.copy(idClosure =
                              idClosureRefinement.getCleansed
                            )
                          )
                        case TxnResultFailure(err) =>
                          completionSignal.complete(Left[Throwable, V](err))
                      }
               } yield ()
             }
      } yield ()
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
        dependencyTally  <- Ref[F].of(0)
        executionStatus  <- Ref[F].of(NotScheduled.asInstanceOf[ExecutionStatus])
        id               <- txnIdGen.getAndUpdate(_ + 1)
        analysedTxn <-
          Async[F].delay(
            AnalysedTxn(
              id = id,
              txn = txn,
              idClosure = staticAnalysisResult._1.getCleansed,
              completionSignal = completionSignal,
              dependencyTally = dependencyTally,
              unsubSpecs = MutableMap(),
              executionStatus = executionStatus
            )
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
