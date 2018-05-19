package de.hpi.svedeb.table

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import de.hpi.svedeb.table.Column.{AppendValue, ValueAppended}
import de.hpi.svedeb.table.Partition._

import scala.concurrent.Future

object Partition {
  case class ListColumnNames()
  case class GetColumn(name: String)
  case class GetColumns()
  case class AddRow(row: RowType, originalSender: ActorRef)

  // Result events
  case class ColumnNameList(columns: Seq[String])
  case class ColumnRetrieved(column: ActorRef)
  case class ColumnsRetrieved(columns: Map[String, ActorRef])
  case class RowAdded(originalSender: ActorRef)
  case class PartitionFull(row: RowType, originalSender: ActorRef)

  def props(id: Int, columnNames: Seq[String] = Seq.empty[String], partitionSize: Int = 10): Props = {
    val columns = columnNames.map(name => (name, ColumnType())).toMap
    Props(new Partition(id, columns, partitionSize))
  }

  def props(id: Int, columns: Map[String, ColumnType], partitionSize: Int): Props = Props(new Partition(id, columns, partitionSize))

  private case class PartitionState(processingInsert: Boolean, rowCount: Int)
}

class Partition(id: Int, columns: Map[String, ColumnType], partitionSize: Int) extends Actor with ActorLogging {
  import context.dispatcher

  // Columns are initialized at actor creation time and cannot be mutated later on.
  private val columnRefs = columns.map { case (name, values) => (name, context.actorOf(Column.props(id, name, values), name)) }

  override def receive: Receive = active(PartitionState(processingInsert = false, 0))

  private def retrieveColumns(): Unit = {
    sender() ! ColumnsRetrieved(columnRefs)
  }

  private def retrieveColumn(name: String): Unit = {
    val column = columnRefs(name)
    sender() ! ColumnRetrieved(column)
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
    } else if (columnRefs.size != row.row.size) {
      val future = Future.failed(new Exception("Wrong number of columns"))
      pipe(future) to sender()
    } else {
      val newState = PartitionState(processingInsert = true, state.rowCount)
      context.become(active(newState))
      addRow(state, row, originalSender)
    }
  }

  private def addRow(state: PartitionState, row: RowType, originalSender: ActorRef): Unit = {
    log.debug("Adding row to partition: {}", row)

    import scala.concurrent.duration._
    implicit val timeout: Timeout = Timeout(5 seconds) // needed for `ask` below

    // TODO: verify that value is appended to correct column
    val listOfFutures = columnRefs.zip(row.row).map { case ((_, column), value) =>
      log.debug("Going to add value {} into column {}", value, column)
      ask(column, AppendValue(value))
    }

    val eventualRowAdded = Future.sequence(listOfFutures)
      .map(f => {
        log.debug("Received all column futures: {}", f)
        val newState = PartitionState(processingInsert = false, state.rowCount + 1)
        context.become(active(newState))
        RowAdded(originalSender)
      })

    pipe(eventualRowAdded) to sender()
  }

  private def active(state: PartitionState): Receive = {
    case ListColumnNames() => listColumns()
    case GetColumn(name) => retrieveColumn(name)
    case GetColumns() => retrieveColumns()
    case AddRow(row, originalSender) =>
      // Postpone message until previous insert is completed
      if (state.processingInsert) self forward AddRow(row, originalSender)
      else tryToAddRow(state, row, originalSender)
    case ValueAppended(partitionId, columnName) => ()
    case m => throw new Exception("Message not understood: " + m)
  }
}
