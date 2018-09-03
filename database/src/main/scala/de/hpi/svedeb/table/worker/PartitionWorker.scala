package de.hpi.svedeb.table.worker

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.table.Column.{ScanColumn, ScanColumnWithOptional, ScannedValues, ScannedValuesWithOptional}
import de.hpi.svedeb.table.OptionalColumnType
import de.hpi.svedeb.table.worker.PartitionWorker.{InternalScanColumns, InternalScannedValues, PartitionWorkerState}
import de.hpi.svedeb.utils.Utils.RowId

object PartitionWorker {

  case class InternalScanColumns(originalSender: ActorRef, indices: Seq[RowId])

  case class InternalScannedValues(originalSender: ActorRef, values: Map[String, OptionalColumnType])

  case class PartitionWorkerState(originalSender: ActorRef, parentPartition: ActorRef, values: Map[String, Option[OptionalColumnType]]) {
    def storeSenders(sender: ActorRef, partition: ActorRef): PartitionWorkerState = {
      PartitionWorkerState(sender, partition, values)
    }

    def storeValues(columnName: String, columnValues: OptionalColumnType): PartitionWorkerState = {
      PartitionWorkerState(originalSender, parentPartition, values + (columnName -> Some(columnValues)))
    }

    def receivedAllColumns: Boolean = {
      values.forall(_._2.isDefined)
    }
  }

  def props(columns: Map[String, ActorRef]): Props = Props(new PartitionWorker(columns))
}

class PartitionWorker(columns: Map[String, ActorRef]) extends Actor with ActorLogging {
  override def receive: Receive = active(PartitionWorkerState(ActorRef.noSender, ActorRef.noSender, columns.keys.map(_ -> None).toMap))

  private def handleScanColumns(state: PartitionWorkerState, originalSender: ActorRef, indices: Seq[RowId]): Unit = {
    // Storing originalSender (the one who requested values) and the partition that created us
    val newState = state.storeSenders(originalSender, sender())
    context.become(active(newState))

    columns.foreach(_._2 ! ScanColumnWithOptional(indices))
  }

  def handleScannedValuesWithOptional(state: PartitionWorkerState, columnName: String, values: OptionalColumnType): Unit = {
    val newState = state.storeValues(columnName, values)
    context.become(active(newState))

    if (newState.receivedAllColumns) {
      newState.parentPartition ! InternalScannedValues(newState.originalSender,
        newState.values.mapValues(_.get).map(identity))
    }
  }

  private def active(state: PartitionWorkerState): Receive = {
    case InternalScanColumns(originalSender, indices) => handleScanColumns(state, originalSender, indices)
    case ScannedValuesWithOptional(_, columnName, values) => handleScannedValuesWithOptional(state, columnName, values)
    case m => throw new Exception(s"Message not understood: $m")
  }

}
