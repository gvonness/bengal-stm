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
package bengal.stm.syntax

import bengal.stm._
import bengal.stm.model._

package object all {

  implicit class TxnVarOps[F[_]: STM, V](txnVar: TxnVar[F, V]) {

    def get: Txn[V] =
      STM[F].getTxnVar(txnVar)

    def set(newValue: => V): Txn[Unit] =
      STM[F].setTxnVar(newValue, txnVar)

    def setF(newValue: F[V]): Txn[Unit] =
      STM[F].setTxnVarF(newValue, txnVar)

    def modify(f: V => V): Txn[Unit] =
      STM[F].modifyTxnVar(f, txnVar)

    def modifyF(f: V => F[V]): Txn[Unit] =
      STM[F].modifyTxnVarF(f, txnVar)
  }

  implicit class TxnVarMapOps[F[_]: STM, K, V](txnVarMap: TxnVarMap[F, K, V]) {

    def get: Txn[Map[K, V]] =
      STM[F].getTxnVarMap(txnVarMap)

    def set(newValueMap: => Map[K, V]): Txn[Unit] =
      STM[F].setTxnVarMap(newValueMap, txnVarMap)

    def set(newValueMap: F[Map[K, V]]): Txn[Unit] =
      STM[F].setTxnVarMapF(newValueMap, txnVarMap)

    def modify(f: Map[K, V] => Map[K, V]): Txn[Unit] =
      STM[F].modifyTxnVarMap(f, txnVarMap)

    def modifyF(f: Map[K, V] => F[Map[K, V]]): Txn[Unit] =
      STM[F].modifyTxnVarMapF(f, txnVarMap)

    def get(key: => K): Txn[Option[V]] =
      STM[F].getTxnVarMapValue(key, txnVarMap)

    def set(key: => K, newValue: => V): Txn[Unit] =
      STM[F].setTxnVarMapValue(key, newValue, txnVarMap)

    def setF(key: => K, newValue: F[V]): Txn[Unit] =
      STM[F].setTxnVarMapValueF(key, newValue, txnVarMap)

    def modify(key: => K, f: V => V): Txn[Unit] =
      STM[F].modifyTxnVarMapValue(key, f, txnVarMap)

    def modifyF(key: => K, f: V => F[V]): Txn[Unit] =
      STM[F].modifyTxnVarMapValueF(key, f, txnVarMap)

    def remove(key: => K): Txn[Unit] =
      STM[F].removeTxnVarMapValue(key, txnVarMap)
  }

  implicit class TxnOps[F[_]: STM, V](txn: => Txn[V]) {

    def commit: F[V] =
      STM[F].commitTxn(txn)

    def handleErrorWith(f: Throwable => Txn[V]): Txn[V] =
      STM[F].handleErrorWithInternal(txn)(f)

    def handleErrorWithF(f: Throwable => F[Txn[V]]): Txn[V] =
      STM[F].handleErrorWithInternalF(txn)(f)
  }
}
