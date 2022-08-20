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

case class TxnVar[F[_]: Async, T](
    private[stm] val id: TxnVarId,
    protected val value: Ref[F, T],
    private[stm] val commitLock: Semaphore[F],
    private[stm] val txnRetrySignals: TxnSignals[F]
) extends TxnStateEntity[F, T] {

  private def completeRetrySignals: F[Unit] =
    for {
      signals <- txnRetrySignals.getAndSet(Set())
      _       <- signals.toList.parTraverse(_.complete(()))
    } yield ()

  private[stm] lazy val get: F[T] =
    value.get

  private[stm] def set(
      newValue: T
  ): F[Unit] =
    for {
      _ <- value.set(newValue)
      _ <- completeRetrySignals
    } yield ()
}

object TxnVar {

  def of[F[_]: STM: Async, T](value: T): F[TxnVar[F, T]] =
    for {
      id       <- STM[F].txnVarIdGen.updateAndGet(_ + 1)
      valueRef <- Async[F].ref(value)
      lock     <- Semaphore[F](1)
      signals  <- Async[F].ref(Set[Deferred[F, Unit]]())
    } yield TxnVar(id, valueRef, lock, signals)
}
