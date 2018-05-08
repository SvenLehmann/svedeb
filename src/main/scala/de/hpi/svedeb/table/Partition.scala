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
  case class AddRow(row: RowType)

  // Result events
  case class ColumnNameList(columns: Seq[String])
  case class ColumnRetrieved(column: ActorRef)
  case class ColumnsRetrieved(columns: Map[String, ActorRef])
  case class RowAdded()
  case class PartitionFull()

  def props(columnNames: Seq[String] = Seq.empty[String], partitionSize: Int = 10): Props = {
    val columns = columnNames.map(name => (name, ColumnType())).toMap
    Props(new Partition(columns, partitionSize))
  }

  def props(columns: Map[String, ColumnType], partitionSize: Int): Props = Props(new Partition(columns, partitionSize))
}

class Partition(columns: Map[String, ColumnType], partitionSize: Int) extends Actor with ActorLogging {
  import context.dispatcher

  // Columns are initialized at actor creation time and cannot be mutated later on.
  private val columnRefs = columns.map { case (name, values) => (name, context.actorOf(Column.props(name, values), name)) }

  override def receive: Receive = active(0)

  def retrieveColumns(): Unit = {
    sender() ! ColumnsRetrieved(columnRefs)
  }

  private def active(rowCount: Int): Receive = {
    case ListColumnNames() => listColumns()
    case GetColumn(name) => retrieveColumn(name)
    case GetColumns() => retrieveColumns()
    case AddRow(row) => tryToAddRow(rowCount, row)
    case ValueAppended() => ()
    case x => log.error("Message not understood: {}", x)
  }

  private def retrieveColumn(name: String): Unit = {
    columnRefs.keys.foreach(s => log.info(s))
    val column = columnRefs(name)
    sender() ! ColumnRetrieved(column)
  }

  private def listColumns(): Unit = {
    log.debug("{}", columnRefs.keys.toSeq)
    val columnNames = columnRefs.keys.toSeq
    sender() ! ColumnNameList(columnNames)
  }

  private def tryToAddRow(rowCount: Int, row: RowType): Unit = {
    if (rowCount >= partitionSize) {
      log.info("Partition full")
      sender() ! PartitionFull()
    } else if (columnRefs.size != row.row.size) {
      val future = Future.failed(new Exception("Wrong number of columns"))
      pipe(future) to sender()
    } else {
      addRow(rowCount, row)
    }
  }

  private def addRow(rowCount: Int, row: RowType): Unit = {
    log.debug("Adding row to partition: {}", row)

    import scala.concurrent.duration._
    implicit val timeout: Timeout = Timeout(5 seconds) // needed for `ask` below

    // TODO: verify that value is appended to correct column
    val listOfFutures = columnRefs.zip(row.row).map { case ((_, column), value) =>
      log.info("Going to add value {} into column {}", value, column)
      ask(column, AppendValue(value))
    }

    val eventualRowAdded = Future.sequence(listOfFutures)
      .map(f => {
        log.info("Received all column futures: {}", f)
        context.become(active(rowCount + 1))
        RowAdded()
      })

    pipe(eventualRowAdded) to sender()
  }

}
