package de.hpi.svedeb.table

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.DataType
import de.hpi.svedeb.table.Column.{AppendValue, ValueAppended}
import de.hpi.svedeb.table.Partition._

object Partition {
  case class ListColumnNames()
  case class GetColumn(name: String)
  case class GetColumns()
  case class AddRow(row: RowType, originalSender: ActorRef)

  // Result events
  case class ColumnNameList(columns: Seq[String])
  case class ColumnRetrieved(partitionId: Int, columnName: String, column: ActorRef)
  case class ColumnsRetrieved(columns: Map[String, ActorRef])
  case class RowAdded(originalSender: ActorRef)
  case class PartitionFull(row: RowType, originalSender: ActorRef)

  def props(partitionId: Int, columnNames: Seq[(String, DataTypeEnum)] = Seq.empty, partitionSize: Int = 10): Props = {
    import de.hpi.svedeb.DataTypeImplicits._
    val columns = columnNames.map(name => (name._1, ColumnTypeFactory.create(name._2))).toMap
    Props(new Partition(partitionId, partitionSize, columns))
  }

  def props(partitionId: Int, columns: Map[String, ColumnType[_]], partitionSize: Int): Props = Props(new Partition(partitionId, partitionSize, columns))

  private case class PartitionState(processingInsert: Boolean, rowCount: Int, remainingColumns: Int, originalSender: ActorRef, tableSender: ActorRef) {
    def decreaseRemainingColumns(): PartitionState = {
      PartitionState(processingInsert, rowCount, remainingColumns - 1, originalSender, tableSender)
    }

    def increaseRowCount(): PartitionState = {
      PartitionState(processingInsert = false, rowCount + 1, 0, originalSender, tableSender)
    }
  }
}

class Partition(partitionId: Int, partitionSize: Int, columns: Map[String, ColumnType[_]] = Map.empty) extends Actor with ActorLogging {

  // Columns are initialized at actor creation time and cannot be mutated later on.
  private val columnRefs = columns.map { case (name, values) => (name, context.actorOf(Column.props(partitionId, name, values), name)) }

  override def receive: Receive = active(PartitionState(processingInsert = false, 0, 0, ActorRef.noSender, ActorRef.noSender))

  private def retrieveColumns(): Unit = {
    sender() ! ColumnsRetrieved(columnRefs)
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
      // TODO: remove StringWitness and use implicit
      import de.hpi.svedeb.DataTypeImplicits._
      import scala.language.implicitConversions
      column ! AppendValue(value)
    }
  }

  def handleValueAppended(state: PartitionState, partitionId: Int, columnName: String) {
    val newState = state.decreaseRemainingColumns()
    context.become(active(newState))

    if (newState.remainingColumns == 0) {
      log.info("remainingColumns zero")
      val increasedRowCountState = newState.increaseRowCount()
      context.become(active(increasedRowCountState))
      state.tableSender ! RowAdded(state.originalSender)
    }
  }

  private def active(state: PartitionState): Receive = {
    case ListColumnNames() => listColumns()
    case GetColumn(name) => retrieveColumn(name)
    case GetColumns() => retrieveColumns()
    case AddRow(row, originalSender) =>
      // Postpone message until previous insert is completed
      if (state.processingInsert) self forward AddRow(row, originalSender)
      else tryToAddRow(state, row, originalSender)
    case ValueAppended(currentPartitionId, columnName) => handleValueAppended(state, currentPartitionId, columnName)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
