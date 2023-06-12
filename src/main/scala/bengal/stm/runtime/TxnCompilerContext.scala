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

import bengal.stm.model.TxnErratum._
import bengal.stm.model._
import bengal.stm.model.runtime._

import cats.arrow.FunctionK
import cats.data.StateT
import cats.effect.kernel.Async
import cats.syntax.all._

private[stm] trait TxnCompilerContext[F[_]] {
  this: AsyncImplicits[F] with TxnLogContext[F] with TxnAdtContext[F] =>

  private[stm] type IdClosureStore[T] = StateT[F, IdClosure, T]
  private[stm] type TxnLogStore[T]    = StateT[F, TxnLog, T]

  private def noOp[S]: StateT[F, S, Unit] =
    StateT[F, S, Unit](s => Async[F].delay((s, ())))

  private[stm] case class StaticAnalysisShortCircuitException(
      idClosure: IdClosure
  ) extends RuntimeException

  private[stm] def staticAnalysisCompiler: FunctionK[TxnOrErr, IdClosureStore] =
    new (FunctionK[TxnOrErr, IdClosureStore]) {

      def apply[V](fa: TxnOrErr[V]): IdClosureStore[V] =
        fa match {
          case Right(entry) =>
            entry match {
              case TxnUnit =>
                noOp[IdClosure].map(_.asInstanceOf[V])
              case TxnDelay(thunk) =>
                StateT[F, IdClosure, V] { s =>
                  thunk.map { materializedValue =>
                    (s, materializedValue)
                  }.handleErrorWith { _ =>
                    Async[F].raiseError(StaticAnalysisShortCircuitException(s))
                  }
                }
              case TxnPure(value) =>
                StateT[F, IdClosure, V](s => Async[F].pure((s, value)))
              case TxnGetVar(txnVar) =>
                StateT[F, IdClosure, V] { s =>
                  txnVar.get.map { v =>
                    (s.addReadId(txnVar.runtimeId), v)
                  }
                }
              case adt: TxnGetVarMap[_, _] =>
                StateT[F, IdClosure, V] { s =>
                  adt.txnVarMap.get
                    .map(v =>
                      (s.addReadId(adt.txnVarMap.runtimeId), v.asInstanceOf[V])
                    )
                }
              case adt: TxnGetVarMapValue[_, _] =>
                StateT[F, IdClosure, V] { s =>
                  adt.key.flatMap { materializedKey =>
                    for {
                      oTxnVar <-
                        adt.txnVarMap.getTxnVar(materializedKey)
                      value <-
                        oTxnVar
                          .map(_.get.map(Some(_)))
                          .getOrElse(Async[F].pure(None))
                      oARId <-
                        adt.txnVarMap.getRuntimeActualisedId(
                          materializedKey
                        )
                      eRId =
                        adt.txnVarMap.getRuntimeExistentialId(
                          materializedKey
                        )
                    } yield oARId
                      .map(id =>
                        (s.addReadId(id).addReadId(eRId), value.asInstanceOf[V])
                      )
                      .getOrElse((s.addReadId(eRId), value.asInstanceOf[V]))
                  }.handleErrorWith { _ =>
                    Async[F].raiseError(StaticAnalysisShortCircuitException(s))
                  }
                }
              case adt: TxnSetVar[_] =>
                StateT[F, IdClosure, V] { s =>
                  Async[F].delay(
                    (s.addWriteId(adt.txnVar.runtimeId), ().asInstanceOf[V])
                  )
                }
              case adt: TxnSetVarMap[_, _] =>
                StateT[F, IdClosure, V] { s =>
                  Async[F].delay(
                    (s.addWriteId(adt.txnVarMap.runtimeId), ().asInstanceOf[V])
                  )
                }
              case adt: TxnSetVarMapValue[_, _] =>
                StateT[F, IdClosure, Unit] { s =>
                  adt.key.flatMap { materializedKey =>
                    for {
                      oARId <-
                        adt.txnVarMap.getRuntimeActualisedId(
                          materializedKey
                        )
                      eRId =
                        adt.txnVarMap.getRuntimeExistentialId(
                          materializedKey
                        )
                    } yield oARId
                      .map(id => (s.addWriteId(id).addWriteId(eRId), ()))
                      .getOrElse((s.addWriteId(eRId), ()))
                  }.handleErrorWith { _ =>
                    Async[F].delay((s, ()))
                  }
                }.map(_.asInstanceOf[V])
              case adt: TxnModifyVarMapValue[_, _] =>
                StateT[F, IdClosure, Unit] { s =>
                  adt.key.flatMap { materializedKey =>
                    for {
                      oARId <-
                        adt.txnVarMap.getRuntimeActualisedId(
                          materializedKey
                        )
                      eRId =
                        adt.txnVarMap.getRuntimeExistentialId(
                          materializedKey
                        )
                    } yield oARId
                      .map(id => (s.addWriteId(id).addWriteId(eRId), ()))
                      .getOrElse((s.addWriteId(eRId), ()))
                  }.handleErrorWith { _ =>
                    Async[F].delay((s, ()))
                  }
                }.map(_.asInstanceOf[V])
              case adt: TxnDeleteVarMapValue[_, _] =>
                StateT[F, IdClosure, Unit] { s =>
                  adt.key.flatMap { materializedKey =>
                    for {
                      oARId <-
                        adt.txnVarMap.getRuntimeActualisedId(
                          materializedKey
                        )
                      eRId =
                        adt.txnVarMap.getRuntimeExistentialId(
                          materializedKey
                        )
                    } yield oARId
                      .map(id => (s.addWriteId(id).addWriteId(eRId), ()))
                      .getOrElse((s.addWriteId(eRId), ()))
                  }.handleErrorWith { _ =>
                    Async[F].delay((s, ()))
                  }
                }.map(_.asInstanceOf[V])
              case adt: TxnHandleError[_] =>
                StateT[F, IdClosure, V] { s =>
                  adt.fa
                    .map(_.map(_.asInstanceOf[V]))
                    .flatMap { materializedF =>
                      materializedF
                        .foldMap(staticAnalysisCompiler)
                        .run(s)
                    }
                    .handleErrorWith { _ =>
                      Async[F].delay((s, ().asInstanceOf[V]))
                    }
                }
              case _ =>
                noOp[IdClosure].map(_.asInstanceOf[V])
            }
          case _ =>
            noOp[IdClosure].map(_.asInstanceOf[V])
        }
    }

  private[stm] lazy val txnLogCompiler: FunctionK[TxnOrErr, TxnLogStore] =
    new (FunctionK[TxnOrErr, TxnLogStore]) {

      def apply[V](fa: TxnOrErr[V]): TxnLogStore[V] =
        fa match {
          case Right(entry) =>
            entry match {
              case TxnUnit =>
                noOp[TxnLog].map(_.asInstanceOf[V])
              case TxnDelay(thunk) =>
                StateT[F, TxnLog, V] { s =>
                  s.delay(thunk)
                }
              case TxnPure(value) =>
                StateT[F, TxnLog, V] { s =>
                  s.pure(value)
                }
              case TxnGetVar(txnVar) =>
                StateT[F, TxnLog, V] { s =>
                  s.getVar(txnVar)
                }
              case adt: TxnSetVar[_] =>
                StateT[F, TxnLog, V] { s =>
                  s.setVar(adt.newValue, adt.txnVar)
                    .map((_, ().asInstanceOf[V]))
                }
              case adt: TxnGetVarMap[_, _] =>
                StateT[F, TxnLog, V] { s =>
                  s.getVarMap(adt.txnVarMap).map { stateAndValue =>
                    (stateAndValue._1, stateAndValue._2.asInstanceOf[V])
                  }
                }
              case adt: TxnGetVarMapValue[_, _] =>
                StateT[F, TxnLog, V] { s =>
                  s.getVarMapValue(adt.key, adt.txnVarMap).map {
                    stateAndValue =>
                      (stateAndValue._1, stateAndValue._2.asInstanceOf[V])
                  }
                }
              case adt: TxnSetVarMap[_, _] =>
                StateT[F, TxnLog, V] { s =>
                  for {
                    newState <-
                      s.setVarMap(adt.newMap, adt.txnVarMap)
                  } yield (newState, ().asInstanceOf[V])
                }
              case adt: TxnSetVarMapValue[_, _] =>
                StateT[F, TxnLog, V] { s =>
                  s.setVarMapValue(adt.key, adt.newValue, adt.txnVarMap)
                    .map((_, ().asInstanceOf[V]))
                }
              case adt: TxnModifyVarMapValue[_, _] =>
                StateT[F, TxnLog, V] { s =>
                  s.modifyVarMapValue(adt.key, adt.f, adt.txnVarMap)
                    .map((_, ().asInstanceOf[V]))
                }
              case adt: TxnDeleteVarMapValue[_, _] =>
                StateT[F, TxnLog, V] { s =>
                  s.deleteVarMapValue(adt.key, adt.txnVarMap)
                    .map((_, ().asInstanceOf[V]))
                }
              case adt: TxnHandleError[_] =>
                StateT[F, TxnLog, V] { s =>
                  (for {
                    materializedF <- adt.fa
                    originalResult <-
                      materializedF.foldMap(txnLogCompiler).run(s)
                    finalResult <- originalResult._1 match {
                                     case TxnLogError(ex) =>
                                       adt
                                         .f(ex)
                                         .flatMap {
                                           _.foldMap(txnLogCompiler)
                                             .run(s)
                                         }
                                     case _ =>
                                       Async[F].pure(originalResult)
                                   }
                  } yield (finalResult._1,
                           finalResult._2.asInstanceOf[V]
                  )).handleErrorWith { ex =>
                    s.raiseError(ex).map((_, ().asInstanceOf[V]))
                  }
                }
              case _ =>
                noOp[TxnLog].map(_.asInstanceOf[V])
            }
          case Left(erratum) =>
            erratum match {
              case TxnRetry =>
                StateT[F, TxnLog, V](
                  _.scheduleRetry.map((_, ().asInstanceOf[V]))
                )
              case TxnError(ex) =>
                StateT[F, TxnLog, V](
                  _.raiseError(ex).map((_, ().asInstanceOf[V]))
                )
              case _ =>
                noOp[TxnLog].map(_.asInstanceOf[V])
            }
        }
    }
}
