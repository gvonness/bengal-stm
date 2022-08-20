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

import cats.effect.{Deferred, Ref}

import scala.collection.mutable.{Map => MutableMap}

package object runtime {
  private[stm] type TxnVarId        = Long
  private[stm] type TxnVarRuntimeId = Int
  private[stm] type TxnId           = Long

  private[stm] type VarIndex[F[_], K, V] = MutableMap[K, TxnVar[F, V]]
  private[stm] type TxnSignals[F[_]]     = Ref[F, Set[Deferred[F, Unit]]]
}
