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

import cats.arrow.FunctionK
import cats.data.StateT
import cats.effect.kernel.Concurrent
import cats.implicits._

import scala.util.{Failure, Success, Try}

private[stm] trait TxnCompilerContext[F[_]] {
  this: TxnAdtContext[F] with TxnStateEntityContext[F] with TxnLogContext[F] =>

  private[stm] type IdClosureStore[T] = StateT[F, IdClosure, T]
  private[stm] type TxnLogStore[T]    = StateT[F, TxnLog, T]

  private def noOp[S](implicit F: Concurrent[F]): StateT[F, S, Unit] =
    StateT[F, S, Unit](s => F.pure((s, ())))

  private[stm] case class StaticAnalysisShortCircuitException(
      idClosure: IdClosure
  ) extends RuntimeException

  private[stm] def staticAnalysisCompiler(implicit
      F: Concurrent[F]
  ): FunctionK[TxnOrErr, IdClosureStore] =
    new (FunctionK[TxnOrErr, IdClosureStore]) {

      def apply[V](fa: TxnOrErr[V]): IdClosureStore[V] =
        fa match {
          case Right(entry) =>
            entry match {
              case TxnUnit =>
                noOp[IdClosure].map(_.asInstanceOf[V])
              case TxnPure(value) =>
                StateT[F, IdClosure, V] { s =>
                  F.pure {
                    Try(value()) match {
                      case Success(materializedValue) =>
                        (s, materializedValue)
                      case _ =>
                        throw StaticAnalysisShortCircuitException(s)
                    }
                  }
                }
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
                  Try(adt.key()) match {
                    case Success(materializedKey) =>
                      for {
                        oTxnVar <-
                          adt.txnVarMap.getTxnVar(materializedKey)
                        value <-
                          oTxnVar
                            .map(_.get.map(Some(_)))
                            .getOrElse(F.pure(None))
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
                          (s.addReadId(id).addReadId(eRId),
                           value.asInstanceOf[V]
                          )
                        )
                        .getOrElse((s.addReadId(eRId), value.asInstanceOf[V]))
                    case _ =>
                      throw StaticAnalysisShortCircuitException(s)
                  }
                }
              case adt: TxnSetVar[_] =>
                StateT[F, IdClosure, V] { s =>
                  F.pure(
                    (s.addWriteId(adt.txnVar.runtimeId), ().asInstanceOf[V])
                  )
                }
              case adt: TxnSetVarMap[_, _] =>
                StateT[F, IdClosure, V] { s =>
                  F.pure(
                    (s.addWriteId(adt.txnVarMap.runtimeId), ().asInstanceOf[V])
                  )
                }
              case adt: TxnSetVarMapValue[_, _] =>
                StateT[F, IdClosure, Unit] { s =>
                  Try(adt.key()) match {
                    case Success(materializedKey) =>
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
                    case _ =>
                      F.pure((s, ()))
                  }
                }.map(_.asInstanceOf[V])
              case adt: TxnModifyVarMapValue[_, _] =>
                StateT[F, IdClosure, Unit] { s =>
                  Try(adt.key()) match {
                    case Success(materializedKey) =>
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
                    case _ =>
                      F.pure((s, ()))
                  }
                }.map(_.asInstanceOf[V])
              case adt: TxnDeleteVarMapValue[_, _] =>
                StateT[F, IdClosure, Unit] { s =>
                  Try(adt.key()) match {
                    case Success(materializedKey) =>
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
                    case _ =>
                      F.pure((s, ()))
                  }
                }.map(_.asInstanceOf[V])
              case adt: TxnHandleError[_] =>
                StateT[F, IdClosure, V] { s =>
                  Try(adt.fa().map(_.asInstanceOf[V])) match {
                    case Success(materializedF) =>
                      materializedF.foldMap(staticAnalysisCompiler).run(s)
                    case _ =>
                      F.pure((s, ().asInstanceOf[V]))
                  }
                }
            }
          case _ =>
            noOp[IdClosure].map(_.asInstanceOf[V])
        }
    }

  private[stm] def txnLogCompiler(implicit
      F: Concurrent[F]
  ): FunctionK[TxnOrErr, TxnLogStore] =
    new (FunctionK[TxnOrErr, TxnLogStore]) {

      def apply[V](fa: TxnOrErr[V]): TxnLogStore[V] =
        fa match {
          case Right(entry) =>
            entry match {
              case TxnUnit =>
                noOp[TxnLog].map(_.asInstanceOf[V])
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
                  Try(adt.fa()) match {
                    case Success(materializedF) =>
                      for {
                        originalResult <-
                          materializedF.foldMap(txnLogCompiler).run(s)
                        finalResult <- originalResult._1 match {
                                         case TxnLogError(ex) =>
                                           adt
                                             .f(ex)
                                             .foldMap(txnLogCompiler)
                                             .run(s)
                                         case _ =>
                                           F.pure(originalResult)
                                       }
                      } yield finalResult
                    case Failure(exception) =>
                      s.raiseError(exception).map((_, ().asInstanceOf[V]))
                  }
                }
            }
          case Left(erratum) =>
            erratum match {
              case TxnRetry =>
                StateT[F, TxnLog, V] {
                  case validLog: TxnLogValid =>
                    throw TxnRetryException(validLog)
                  case s =>
                    F.pure((s, ().asInstanceOf[V]))
                }
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
