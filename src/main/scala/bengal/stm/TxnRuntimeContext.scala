/*
 * Copyright 2020-2021 Entrolution
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

import bengal.stm.TxnCompilerContext.IdClosure
import bengal.stm.TxnStateEntityContext.TxnId

import cats.effect.implicits._
import cats.effect.kernel.{Concurrent, Temporal}
import cats.effect.std.Semaphore
import cats.effect.{Deferred, Ref}
import cats.implicits._

import scala.annotation.nowarn
import scala.collection.mutable.{ListBuffer, Map => MutableMap}
import scala.concurrent.duration.FiniteDuration

private[stm] trait TxnRuntimeContext[F[_]] {
  this: TxnAdtContext[F] with TxnCompilerContext[F] with TxnLogContext[F] =>

  private[stm] val txnIdGen: Ref[F, TxnId]

  private[stm] sealed trait TxnResult
  private[stm] case class TxnResultSuccess[V](result: V) extends TxnResult

  private[stm] case class TxnResultRetry(
      registerRetry: Deferred[F, Unit] => F[Unit]
  ) extends TxnResult

  private[stm] object TxnResultRetry {

    private[stm] def apply(logRetry: TxnLogRetry): TxnResultRetry =
      TxnResultRetry(logRetry.registerRetry)
  }
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
      schedulerTrigger: Ref[F, Deferred[F, Unit]],
      retryWaitMaxDuration: FiniteDuration
  ) {

    private def withRunningLock[A](fa: F[A])(implicit F: Concurrent[F]): F[A] =
      runningSemaphore.permit.use(_ => fa)

    private[stm] def triggerReprocessing(implicit F: Concurrent[F]): F[Unit] =
      schedulerTrigger.get.flatMap(_.complete(())).void

    private[stm] def registerCompletion(
        txnId: TxnId
    )(implicit F: Concurrent[F]): F[Unit] =
      for {
        _                <- waitingSemaphore.acquire
        _                <- withRunningLock(F.pure(runningMap -= txnId))
        waitingPopulated <- F.pure(waitingBuffer.nonEmpty)
        _ <- if (waitingPopulated)
               triggerReprocessing
             else {
               F.unit
             }
        _ <- waitingSemaphore.release
      } yield ()

    private[stm] def submitTxn[V](
        analysedTxn: AnalysedTxn[V]
    )(implicit F: Concurrent[F]): F[Unit] =
      for {
        _ <- waitingSemaphore.acquire
        _ <- F.pure(waitingBuffer.append(analysedTxn))
        _ <- triggerReprocessing
        _ <- waitingSemaphore.release
      } yield ()

    private def attemptExecution(
        analysedTxn: AnalysedTxn[_]
    )(implicit F: Concurrent[F], TF: Temporal[F]): F[Unit] =
      for {
        _ <- withRunningLock {
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
                    for {
                      _ <- runningSemaphore.release
                    } yield IdClosure.empty
                  }
      } yield result

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
               bufferLocked   <- F.pure(waitingBuffer.toList)
               _              <- F.pure(waitingBuffer.clear())
               runningClosure <- getRunningClosure
               _ <- bufferLocked.foldLeftM(runningClosure) { (i, j) =>
                      for {
                        _ <- if (j.idClosure.isCompatibleWith(i)) {
                               attemptExecution(j)
                             } else {
                               F.pure(waitingBuffer.append(j))
                             }
                      } yield i.mergeWith(j.idClosure)
                    }
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
        retryWaitMaxDuration: FiniteDuration
    ): TxnScheduler =
      TxnScheduler(
        runningMap = MutableMap(),
        runningSemaphore,
        waitingBuffer = ListBuffer(),
        waitingSemaphore,
        schedulerTrigger,
        retryWaitMaxDuration
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
    ): F[(TxnLog, V)] =
      txn.foldMap[TxnLogStore](txnLogCompiler).run(TxnLogValid.empty)

    private[stm] def commit(implicit F: Concurrent[F]): F[TxnResult] =
      for {
        logResult <- getTxnLogResult
        (log, logValue) = logResult
        result <- log match {
                    case TxnLogValid(_) =>
                      F.uncancelable { _ =>
                        log.withLock {
                          F.ifM[Option[V]](log.isDirty)(
                            F.pure(None),
                            log.commit.as(Some(logValue))
                          )
                        }
                      }.flatMap { s =>
                        s.map(v =>
                          F.pure(TxnResultSuccess(v).asInstanceOf[TxnResult])
                        ).getOrElse(commit)
                      }
                    case retry @ TxnLogRetry(_) =>
                      F.pure(TxnResultRetry(retry))
                    case err @ TxnLogError(_) =>
                      F.pure(TxnResultFailure(err))
                  }
      } yield result

    private[stm] def execute(
        ex: TxnScheduler
    )(implicit F: Concurrent[F], TF: Temporal[F]): F[Unit] =
      for {
        result <- commit
        _ <- result match {
               case TxnResultSuccess(result) =>
                 for {
                   _ <- completionSignal.complete(
                          Right[Throwable, V](result.asInstanceOf[V])
                        )
                   _ <- ex.registerCompletion(id)
                 } yield ()
               case TxnResultRetry(retryCallback) =>
                 for {
                   _               <- ex.registerCompletion(id)
                   reattemptSignal <- Deferred[F, Unit]
                   _               <- retryCallback(reattemptSignal)
                   _ <- F.race(reattemptSignal.get,
                               TF.sleep(ex.retryWaitMaxDuration)
                        )
                   _ <- ex.submitTxn(this)
                 } yield ()
               case TxnResultFailure(err) =>
                 for {
                   _ <- completionSignal.complete(Left[Throwable, V](err))
                   _ <- ex.registerCompletion(id)
                 } yield ()
             }
      } yield ()

  }

  private[stm] trait TxnRuntime {

    private[stm] val scheduler: TxnScheduler

    private[stm] def commit[V](txn: Txn[V])(implicit F: Concurrent[F]): F[V] =
      for {
        staticAnalysisResult <-
          txn
            .foldMap[IdClosureStore](staticAnalysisCompiler)
            .run(IdClosure.empty)
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
