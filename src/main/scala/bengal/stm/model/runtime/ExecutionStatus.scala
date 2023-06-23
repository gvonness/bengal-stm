package ai.entrolution
package bengal.stm.model.runtime

sealed trait ExecutionStatus

case object Scheduled extends ExecutionStatus
case object Running extends ExecutionStatus
case object NotScheduled extends ExecutionStatus
