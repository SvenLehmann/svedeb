package de.hpi.svedeb.table

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import de.hpi.svedeb.table.Column.{AppendValue, ValueAppended}
import de.hpi.svedeb.table.Partition._
import de.hpi.svedeb.table.worker.PartitionWorker
import de.hpi.svedeb.table.worker.PartitionWorker.{InternalScanColumns, InternalScannedValues}
import de.hpi.svedeb.utils.Utils
import de.hpi.svedeb.utils.Utils.RowId


/**
  * TODO: Improve transactional query processing of this actor
  * Adding rows blocks the Partition Actor.
  * However, for now we focus on analytical workloads that are read-only.
  * Supporting transactional queries properly would require proper transactions.
  */

object Partition {
  case class ListColumnNames()
  case class GetColumn(name: String)
  case class GetColumns()
  case class ScanColumns(indices: Seq[RowId])
  case class AddRow(row: RowType, originalSender: ActorRef)

  // Result events
  case class ColumnNameList(columns: Seq[String])
//  case class ScannedColumns(indices: Option[Seq[RowId]] = None)
  case class ScannedColumns(partitionId: Int, columns: Map[String, OptionalColumnType])
  case class ColumnRetrieved(partitionId: Int, columnName: String, column: ActorRef)
  case class ColumnsRetrieved(partitionId: Int, columns: Map[String, ActorRef])
  case class RowAdded(originalSender: ActorRef)
  case class PartitionFull(row: RowType, originalSender: ActorRef)

  def props(partitionId: Int,
            columns: Map[String, ColumnType],
            partitionSize: Int = Utils.defaultPartitionSize): Props = Props(new Partition(partitionId, partitionSize, columns))

  private case class PartitionState(processingInsert: Boolean,
                                    rowCount: Int,
                                    remainingColumns: Int,
                                    originalSender: ActorRef, tableSender: ActorRef) {
    def decreaseRemainingColumns(): PartitionState = {
      PartitionState(processingInsert, rowCount, remainingColumns - 1, originalSender, tableSender)
    }

    def increaseRowCount(): PartitionState = {
      PartitionState(processingInsert = false, rowCount + 1, 0, originalSender, tableSender)
    }
  }
}

class Partition(partitionId: Int,
                partitionSize: Int,
                columns: Map[String, ColumnType] = Map.empty) extends Actor with ActorLogging {

  // Columns are initialized at actor creation time and cannot be mutated later on.
  private val columnRefs = columns.map {
    case (name, values) => (name, context.actorOf(Column.props(partitionId, name, values), name))
  }

  override def receive: Receive = active(PartitionState(processingInsert = false, 0, 0, ActorRef.noSender, ActorRef.noSender))

  private def retrieveColumns(): Unit = {
    sender() ! ColumnsRetrieved(partitionId, columnRefs)
  }

  private def retrieveColumn(name: String): Unit = {
    val column = columnRefs(name)
    sender() ! ColumnRetrieved(partitionId, name, column)
  }

  private def listColumns(): Unit = {
    log.debug("{}", columnRefs.keys.toSeq)
    val columnNames = columnRefs.keys.toSeq
    sender() ! ColumnNameList(columnNames)
  }

  private def tryToAddRow(state: PartitionState, row: RowType, originalSender: ActorRef): Unit = {
    if (state.rowCount >= partitionSize) {
      log.debug("Partition full {}", row.row)
      sender() ! PartitionFull(row, originalSender)
    } else {
      val newState = PartitionState(processingInsert = true, state.rowCount, row.row.size, originalSender, sender())
      context.become(active(newState))
      addRow(state, row, originalSender)
    }
  }

  private def addRow(state: PartitionState, row: RowType, originalSender: ActorRef): Unit = {
    log.debug("Adding row to partition: {}", row)

    // TODO: verify that value is appended to correct column
    columnRefs.zip(row.row).foreach { case ((_, column), value) =>
      log.debug("Going to add value {} into column {}", value, column)
      column ! AppendValue(value)
    }
  }

  def handleValueAppended(state: PartitionState, columnName: String) {
    val newState = state.decreaseRemainingColumns()
    context.become(active(newState))

    if (newState.remainingColumns == 0) {
      log.debug("remainingColumns zero")
      val increasedRowCountState = newState.increaseRowCount()
      context.become(active(increasedRowCountState))
      state.tableSender ! RowAdded(state.originalSender)
    }
  }

  private def handleScanColumns(state: PartitionState, indices: Seq[RowId]): Unit = {
    val worker = context.actorOf(PartitionWorker.props(columnRefs))
    worker ! InternalScanColumns(sender(), indices)
  }

  private def active(state: PartitionState): Receive = {
    case ListColumnNames() => listColumns()
    case GetColumn(name) => retrieveColumn(name)
    case GetColumns() => retrieveColumns()
    case ScanColumns(indices) => handleScanColumns(state, indices)
    case InternalScannedValues(originalSender, values) =>
      log.debug(s"partition $partitionId received InternalScannedValues from PartitionWorker")
      sender() ! PoisonPill
      originalSender ! ScannedColumns(partitionId, values)
    case AddRow(row, originalSender) =>
      // Postpone message until previous insert is completed
      if (state.processingInsert) self forward AddRow(row, originalSender)
      else tryToAddRow(state, row, originalSender)
    case ValueAppended(_, columnName) => handleValueAppended(state, columnName)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
