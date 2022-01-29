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
import bengal.stm.TxnStateEntityContext.TxnVarRuntimeId

import cats.arrow.FunctionK
import cats.data.StateT
import cats.effect.kernel.Concurrent
import cats.implicits._

private[stm] trait TxnCompilerContext[F[_]] {
  this: TxnAdtContext[F] with TxnStateEntityContext[F] with TxnLogContext[F] =>

  private[stm] type IdClosureStore[T] = StateT[F, IdClosure, T]
  private[stm] type TxnErrorStore[T]  = StateT[F, TxnErratum, T]
  private[stm] type TxnLogStore[T]    = StateT[F, TxnLog, T]

  private def noOp[S](implicit F: Concurrent[F]): StateT[F, S, Unit] =
    StateT[F, S, Unit](s => F.pure((s, ())))

  private def voidMapping[S, V](
      fa: TxnOrErr[V]
  )(implicit F: Concurrent[F]): StateT[F, S, V] =
    fa match {
      case Right(TxnUnit) =>
        noOp[S].map(_.asInstanceOf[V])
      case Right(TxnPure(value)) =>
        StateT[F, S, V](s => F.pure((s, value)))
      case Right(TxnGetVar(txnVar)) =>
        StateT[F, S, V](s => txnVar.get.map(v => (s, v)))
      case Right(_: TxnSetVar[_]) =>
        noOp[S].map(_.asInstanceOf[V])
      case Right(adt: TxnGetVarMap[_, _]) =>
        StateT[F, S, V](s => adt.txnVarMap.get.map(v => (s, v.asInstanceOf[V])))
      case Right(adt: TxnGetVarMapValue[_, _]) =>
        StateT[F, S, V](s =>
          adt.txnVarMap.get.map(v => (s, v.get(adt.key).asInstanceOf[V]))
        )
      case Right(_: TxnSetVarMap[_, _]) =>
        noOp[S].map(_.asInstanceOf[V])
      case Right(_: TxnSetVarMapValue[_, _]) =>
        noOp[S].map(_.asInstanceOf[V])
      case Right(_: TxnModifyVarMapValue[_, _]) =>
        noOp[S].map(_.asInstanceOf[V])
      case Right(_: TxnDeleteVarMapValue[_, _]) =>
        noOp[S].map(_.asInstanceOf[V])
      case Right(_: TxnHandleError[_]) =>
        noOp[S].map(_.asInstanceOf[V])
      case Left(_) =>
        noOp[S].map(_.asInstanceOf[V])
    }

  private[stm] def staticAnalysisCompiler(implicit
      F: Concurrent[F]
  ): FunctionK[TxnOrErr, IdClosureStore] =
    new (FunctionK[TxnOrErr, IdClosureStore]) {

      def apply[V](fa: TxnOrErr[V]): IdClosureStore[V] =
        fa match {
          case Right(TxnGetVar(txnVar)) =>
            StateT[F, IdClosure, V] { s =>
              txnVar.get.map { v =>
                (s.addReadId(txnVar.runtimeId), v)
              }
            }
          case Right(adt: TxnGetVarMap[_, _]) =>
            StateT[F, IdClosure, V] { s =>
              adt.txnVarMap.get.map(v =>
                (s.addReadId(adt.txnVarMap.runtimeId), v.asInstanceOf[V])
              )
            }
          case Right(adt: TxnGetVarMapValue[_, _]) =>
            StateT[F, IdClosure, V] { s =>
              for {
                oTxnVar <- adt.txnVarMap.getTxnVar(adt.key)
                value   <- oTxnVar.map(_.get.map(Some(_))).getOrElse(F.pure(None))
                oARId   <- adt.txnVarMap.getRuntimeActualisedId(adt.key)
                eRId = adt.txnVarMap.getRuntimeExistentialId(adt.key)
              } yield oARId
                .map(id =>
                  (s.addReadId(id).addReadId(eRId), value.asInstanceOf[V])
                )
                .getOrElse((s.addReadId(eRId), value.asInstanceOf[V]))
            }
          case Right(adt: TxnSetVar[_]) =>
            StateT[F, IdClosure, Unit] { s =>
              F.pure((s.addWriteId(adt.txnVar.runtimeId), ()))
            }.map(_.asInstanceOf[V])
          case Right(adt: TxnSetVarMap[_, _]) =>
            StateT[F, IdClosure, Unit] { s =>
              F.pure((s.addWriteId(adt.txnVarMap.runtimeId), ()))
            }.map(_.asInstanceOf[V])
          case Right(adt: TxnSetVarMapValue[_, _]) =>
            StateT[F, IdClosure, Unit] { s =>
              for {
                oARId <- adt.txnVarMap.getRuntimeActualisedId(adt.key)
                eRId = adt.txnVarMap.getRuntimeExistentialId(adt.key)
              } yield oARId
                .map(id => (s.addWriteId(id).addWriteId(eRId), ()))
                .getOrElse((s.addWriteId(eRId), ()))
            }.map(_.asInstanceOf[V])
          case Right(adt: TxnModifyVarMapValue[_, _]) =>
            StateT[F, IdClosure, Unit] { s =>
              for {
                oARId <- adt.txnVarMap.getRuntimeActualisedId(adt.key)
                eRId = adt.txnVarMap.getRuntimeExistentialId(adt.key)
              } yield oARId
                .map(id => (s.addWriteId(id).addWriteId(eRId), ()))
                .getOrElse((s.addWriteId(eRId), ()))
            }.map(_.asInstanceOf[V])
          case Right(adt: TxnDeleteVarMapValue[_, _]) =>
            StateT[F, IdClosure, Unit] { s =>
              for {
                oARId <- adt.txnVarMap.getRuntimeActualisedId(adt.key)
                eRId = adt.txnVarMap.getRuntimeExistentialId(adt.key)
              } yield oARId
                .map(id => (s.addWriteId(id).addWriteId(eRId), ()))
                .getOrElse((s.addWriteId(eRId), ()))
            }.map(_.asInstanceOf[V])
          case Right(adt: TxnHandleError[_]) =>
            StateT[F, IdClosure, V] { s =>
              adt.fa.foldMap(staticAnalysisCompiler).run(s)
            }
          case _ =>
            voidMapping[IdClosure, V](fa)
        }
    }

  private[stm] def txnLogCompiler(implicit
      F: Concurrent[F]
  ): FunctionK[TxnOrErr, TxnLogStore] =
    new (FunctionK[TxnOrErr, TxnLogStore]) {

      def apply[V](fa: TxnOrErr[V]): TxnLogStore[V] =
        fa match {
          case Right(TxnGetVar(txnVar)) =>
            StateT[F, TxnLog, V] { s =>
              for {
                newState <- s.getVar(txnVar)
                value    <- newState.getCurrentValue(txnVar)
                result <- value match {
                            case Some(v) => F.pure((newState, v))
                            case None =>
                              for {
                                fallbackValue <- txnVar.get
                              } yield (newState, fallbackValue)
                          }
              } yield result
            }
          case Right(adt: TxnSetVar[_]) =>
            StateT[F, TxnLog, V] { s =>
              for {
                newState <- s.setVar(adt.newValue, adt.txnVar)
              } yield (newState, ())
            }
          case Right(adt: TxnGetVarMap[_, _]) =>
            StateT[F, TxnLog, V] { s =>
              for {
                immutableMap <- adt.txnVarMap.get
                newState <- immutableMap.keySet.toList.foldLeftM(s) { (i, j) =>
                              i.getVarMapValue(j, adt.txnVarMap)
                            }
              } yield (newState, newState.getCurrentValue(adt.txnVarMap))
            }
          case Right(adt: TxnGetVarMapValue[_, _]) =>
            StateT[F, TxnLog, V] { s =>
              for {
                newState <- s.getVarMapValue(adt.key, adt.txnVarMap)
                value    <- newState.getCurrentValue(adt.key, adt.txnVarMap)
              } yield (newState, value.asInstanceOf[V])
            }
          case Right(adt: TxnSetVarMap[_, _]) =>
            StateT[F, TxnLog, Unit] { log =>
              for {
                deleted <-
                  adt.txnVarMap.get.map(m =>
                    m.keySet ++ log
                      .getCurrentValue(adt.txnVarMap)
                      .keySet -- adt.newMap.keySet
                  )
                additions <- adt.newMap.toList.foldLeftM(log) { (i, j) =>
                               i.setVarMapValue(j._1, j._2, adt.txnVarMap)
                             }
                result <- deleted.toList.foldLeftM(additions) { (i, j) =>
                            i.deleteVarMapValue(j, adt.txnVarMap)
                          }
              } yield (result, ())
            }.map(_.asInstanceOf[V])
          case Right(adt: TxnSetVarMapValue[_, _]) =>
            StateT[F, TxnLog, Unit] { s =>
              for {
                newState <-
                  s.setVarMapValue(adt.key, adt.newValue, adt.txnVarMap)
              } yield (newState, ())
            }.map(_.asInstanceOf[V])
          case Right(adt: TxnModifyVarMapValue[_, _]) =>
            StateT[F, TxnLog, Unit] { s =>
              for {
                newState <-
                  s.modifyVarMapValue(adt.key, adt.f, adt.txnVarMap)
              } yield (newState, ())
            }.map(_.asInstanceOf[V])
          case Right(adt: TxnDeleteVarMapValue[_, _]) =>
            StateT[F, TxnLog, Unit] { s =>
              for {
                newState <- s.deleteVarMapValue(adt.key, adt.txnVarMap)
              } yield (newState, ())
            }.map(_.asInstanceOf[V])
          case Right(adt: TxnHandleError[_]) =>
            StateT[F, TxnLog, V] { s =>
              for {
                originalResult <- adt.fa.foldMap(txnLogCompiler).run(s)
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
            }
          case Left(TxnRetry) =>
            StateT[F, TxnLog, Unit] { s =>
              for {
                newState <- s.scheduleRetry
              } yield (newState, ())
            }.map(_.asInstanceOf[V])
          case Left(TxnError(ex)) =>
            StateT[F, TxnLog, Unit] { s =>
              for {
                newState <- s.raiseError(ex)
              } yield (newState, ())
            }.map(_.asInstanceOf[V])
          case _ =>
            voidMapping[TxnLog, V](fa)
        }
    }
}

private[stm] object TxnCompilerContext {

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
}
