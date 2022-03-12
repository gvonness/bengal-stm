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

private[stm] trait TxnCompilerContext[F[_]] {
  this: TxnAdtContext[F] with TxnStateEntityContext[F] with TxnLogContext[F] =>

  private[stm] type IdClosureStore[T] = StateT[F, IdClosure, T]
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
        StateT[F, S, V] { s =>
          try {
            val materializedValue = value()

            F.pure((s, materializedValue))
          } catch {
            case _: Throwable =>
              F.pure((s, ().asInstanceOf[V]))
          }
        }
      case Right(TxnGetVar(txnVar)) =>
        StateT[F, S, V] { s =>
          try {
            val materializedVar = txnVar()

            materializedVar.get.map(v => (s, v))
          } catch {
            case _: Throwable =>
              F.pure((s, ().asInstanceOf[V]))
          }
        }
      case Right(_: TxnSetVar[_]) =>
        noOp[S].map(_.asInstanceOf[V])
      case Right(adt: TxnGetVarMap[_, _]) =>
        StateT[F, S, V] { s =>
          try {
            val materializedVarMap = adt.txnVarMap()

            materializedVarMap.get.map(v => (s, v.asInstanceOf[V]))
          } catch {
            case _: Throwable =>
              F.pure((s, ().asInstanceOf[V]))
          }
        }
      case Right(adt: TxnGetVarMapValue[_, _]) =>
        StateT[F, S, V] { s =>
          try {
            val materializedVarMap = adt.txnVarMap()
            val materializedKey    = adt.key()

            materializedVarMap.get.map(v =>
              (s, v.get(materializedKey).asInstanceOf[V])
            )
          } catch {
            case _: Throwable =>
              F.pure((s, ().asInstanceOf[V]))
          }
        }
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
              try {
                val materializedVar = txnVar()

                materializedVar.get.map { v =>
                  (s.addReadId(materializedVar.runtimeId), v)
                }
              } catch {
                case _: Throwable =>
                  F.pure((s, ().asInstanceOf[V]))
              }
            }
          case Right(adt: TxnGetVarMap[_, _]) =>
            StateT[F, IdClosure, V] { s =>
              try {
                val materializedVarMap = adt.txnVarMap()

                materializedVarMap.get
                  .map(v =>
                    (s.addReadId(materializedVarMap.runtimeId),
                     v.asInstanceOf[V]
                    )
                  )
              } catch {
                case _: Throwable =>
                  F.pure((s, ().asInstanceOf[V]))
              }
            }
          case Right(adt: TxnGetVarMapValue[_, _]) =>
            StateT[F, IdClosure, V] { s =>
              try {
                val materializedVarMap = adt.txnVarMap()
                val materializedKey    = adt.key()

                for {
                  oTxnVar <- materializedVarMap.getTxnVar(materializedKey)
                  value <-
                    oTxnVar.map(_.get.map(Some(_))).getOrElse(F.pure(None))
                  oARId <-
                    materializedVarMap.getRuntimeActualisedId(materializedKey)
                  eRId =
                    materializedVarMap.getRuntimeExistentialId(materializedKey)
                } yield oARId
                  .map(id =>
                    (s.addReadId(id).addReadId(eRId), value.asInstanceOf[V])
                  )
                  .getOrElse((s.addReadId(eRId), value.asInstanceOf[V]))
              } catch {
                case _: Throwable =>
                  F.pure((s, ().asInstanceOf[V]))
              }
            }
          case Right(adt: TxnSetVar[_]) =>
            StateT[F, IdClosure, Unit] { s =>
              try {
                val materializedVar = adt.txnVar()

                F.pure((s.addWriteId(materializedVar.runtimeId), ()))
              } catch {
                case _: Throwable => F.pure((s, ()))
              }
            }.map(_.asInstanceOf[V])
          case Right(adt: TxnSetVarMap[_, _]) =>
            StateT[F, IdClosure, Unit] { s =>
              try {
                val materializedVarMap = adt.txnVarMap()

                F.pure((s.addWriteId(materializedVarMap.runtimeId), ()))
              } catch {
                case _: Throwable => F.pure((s, ()))
              }
            }.map(_.asInstanceOf[V])
          case Right(adt: TxnSetVarMapValue[_, _]) =>
            StateT[F, IdClosure, Unit] { s =>
              try {
                val materializedVarMap = adt.txnVarMap()
                val materializedKey    = adt.key()

                for {
                  oARId <-
                    materializedVarMap.getRuntimeActualisedId(materializedKey)
                  eRId =
                    materializedVarMap.getRuntimeExistentialId(materializedKey)
                } yield oARId
                  .map(id => (s.addWriteId(id).addWriteId(eRId), ()))
                  .getOrElse((s.addWriteId(eRId), ()))
              } catch {
                case _: Throwable => F.pure((s, ()))
              }
            }.map(_.asInstanceOf[V])
          case Right(adt: TxnModifyVarMapValue[_, _]) =>
            StateT[F, IdClosure, Unit] { s =>
              try {
                val materializedVarMap = adt.txnVarMap()
                val materializedKey    = adt.key()

                for {
                  oARId <-
                    materializedVarMap.getRuntimeActualisedId(materializedKey)
                  eRId =
                    materializedVarMap.getRuntimeExistentialId(materializedKey)
                } yield oARId
                  .map(id => (s.addWriteId(id).addWriteId(eRId), ()))
                  .getOrElse((s.addWriteId(eRId), ()))
              } catch {
                case _: Throwable => F.pure((s, ()))
              }
            }.map(_.asInstanceOf[V])
          case Right(adt: TxnDeleteVarMapValue[_, _]) =>
            StateT[F, IdClosure, Unit] { s =>
              try {
                val materializedVarMap = adt.txnVarMap()
                val materializedKey    = adt.key()

                for {
                  oARId <-
                    materializedVarMap.getRuntimeActualisedId(materializedKey)
                  eRId =
                    materializedVarMap.getRuntimeExistentialId(materializedKey)
                } yield oARId
                  .map(id => (s.addWriteId(id).addWriteId(eRId), ()))
                  .getOrElse((s.addWriteId(eRId), ()))
              } catch {
                case _: Throwable => F.pure((s, ()))
              }
            }.map(_.asInstanceOf[V])
          case Right(adt: TxnHandleError[_]) =>
            StateT[F, IdClosure, V] { s =>
              try {
                val materializedF: Txn[V] = adt.fa().map(_.asInstanceOf[V])

                materializedF.foldMap(staticAnalysisCompiler).run(s)
              } catch {
                case _: Throwable =>
                  F.pure((s, ().asInstanceOf[V]))
              }
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
              try {
                val materializedVar = txnVar()

                for {
                  newState <- s.getVar(materializedVar)
                  value    <- newState.getCurrentValue(materializedVar)
                  result <- value match {
                              case Some(v) => F.pure((newState, v))
                              case None =>
                                for {
                                  fallbackValue <- materializedVar.get
                                } yield (newState, fallbackValue)
                            }
                } yield result
              } catch {
                case ex: Throwable =>
                  s.raiseError(ex).map((_, ().asInstanceOf[V]))
              }
            }
          case Right(adt: TxnSetVar[_]) =>
            StateT[F, TxnLog, Unit] { s =>
              try {
                val materializedVar      = adt.txnVar()
                val materializedNewValue = adt.newValue()

                s.setVar(materializedNewValue, materializedVar)
                  .map(
                    (_, ())
                  )
              } catch {
                case ex: Throwable =>
                  s.raiseError(ex).map((_, ()))
              }
            }.map(_.asInstanceOf[V])
          case Right(adt: TxnGetVarMap[_, _]) =>
            StateT[F, TxnLog, V] { s =>
              try {
                val materializedVarMap = adt.txnVarMap()

                for {
                  newState <- s.getVarMap(materializedVarMap)
                  newValue <- newState.getCurrentValue(materializedVarMap)
                } yield (newState, newValue)
              } catch {
                case ex: Throwable =>
                  s.raiseError(ex).map((_, ().asInstanceOf[V]))
              }
            }
          case Right(adt: TxnGetVarMapValue[_, _]) =>
            StateT[F, TxnLog, V] { s =>
              try {
                val materializedVarMap = adt.txnVarMap()
                val materializedKey    = adt.key()

                for {
                  newState <-
                    s.getVarMapValue(materializedKey, materializedVarMap)
                  value <- newState.getCurrentValue(materializedKey,
                                                    materializedVarMap
                           )
                } yield (newState, value.asInstanceOf[V])
              } catch {
                case ex: Throwable =>
                  s.raiseError(ex).map((_, ().asInstanceOf[V]))
              }
            }
          case Right(adt: TxnSetVarMap[_, _]) =>
            StateT[F, TxnLog, Unit] { s =>
              try {
                val materializedVarMap    = adt.txnVarMap()
                val materializedNewVarMap = adt.newMap()

                for {
                  newState <-
                    s.setVarMap(materializedNewVarMap, materializedVarMap)
                } yield (newState, ())
              } catch {
                case ex: Throwable =>
                  s.raiseError(ex).map((_, ()))
              }
            }.map(_.asInstanceOf[V])
          case Right(adt: TxnSetVarMapValue[_, _]) =>
            StateT[F, TxnLog, Unit] { s =>
              try {
                val materializedVarMap   = adt.txnVarMap()
                val materializedKey      = adt.key()
                val materializedNewValue = adt.newValue()

                s.setVarMapValue(materializedKey,
                                 materializedNewValue,
                                 materializedVarMap
                ).map(
                  (_, ())
                )
              } catch {
                case ex: Throwable =>
                  s.raiseError(ex).map((_, ()))
              }
            }.map(_.asInstanceOf[V])
          case Right(adt: TxnModifyVarMapValue[_, _]) =>
            StateT[F, TxnLog, Unit] { s =>
              try {
                val materializedVarMap = adt.txnVarMap()
                val materializedKey    = adt.key()

                s.modifyVarMapValue(materializedKey, adt.f, materializedVarMap)
                  .map(
                    (_, ())
                  )
              } catch {
                case ex: Throwable =>
                  s.raiseError(ex).map((_, ()))
              }
            }.map(_.asInstanceOf[V])
          case Right(adt: TxnDeleteVarMapValue[_, _]) =>
            StateT[F, TxnLog, Unit] { s =>
              try {
                val materializedVarMap = adt.txnVarMap()
                val materializedKey    = adt.key()

                s.deleteVarMapValue(materializedKey, materializedVarMap)
                  .map((_, ()))
              } catch {
                case ex: Throwable =>
                  s.raiseError(ex).map((_, ()))
              }
            }.map(_.asInstanceOf[V])
          case Right(adt: TxnHandleError[_]) =>
            StateT[F, TxnLog, V] { s =>
              try {
                val materializedF = adt.fa()

                for {
                  originalResult <- materializedF.foldMap(txnLogCompiler).run(s)
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
              } catch {
                case ex: Throwable =>
                  s.raiseError(ex).map((_, ().asInstanceOf[V]))
              }
            }
          case Left(TxnRetry) =>
            StateT[F, TxnLog, Unit](_.scheduleRetry.map((_, ())))
              .map(_.asInstanceOf[V])
          case Left(TxnError(ex)) =>
            StateT[F, TxnLog, Unit](_.raiseError(ex).map((_, ())))
              .map(_.asInstanceOf[V])
          case _ =>
            voidMapping[TxnLog, V](fa)
        }
    }
}
