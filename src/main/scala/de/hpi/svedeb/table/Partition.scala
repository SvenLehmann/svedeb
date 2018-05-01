package de.hpi.svedeb.table

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import de.hpi.svedeb.table.Column.{AppendValue, ValueAppended}
import de.hpi.svedeb.table.Partition._

import scala.concurrent.Future

object Partition {
  case class ListColumns()
  case class GetColumn(name: String)
  case class AddRow(row: List[String])

  // Result events
  case class ColumnList(columns: List[String])
  case class RetrievedColumn(column: ActorRef)
  case class RowAdded()
  case class PartitionFull()

  def props(columns: List[String] = List.empty[String], partitionSize: Int = 10): Props = Props(new Partition(columns, partitionSize))
}

class Partition(columnNames: List[String], partitionSize: Int) extends Actor with ActorLogging {
  import context.dispatcher

  // Columns are initialized at actor creation time and cannot be mutated later on.
  private val columnRefs = columnNames.map(name => context.actorOf(Column.props(name), name))

  override def receive: Receive = active(0)

  private def active(rowCount: Int): Receive = {
    case ListColumns() => listColumns()
    case GetColumn(name) => retrieveColumn(name)
    case AddRow(row) => tryToAddRow(rowCount, row)
    case ValueAppended() => ()
    case x => log.error("Message not understood: {}", x)
  }

  private def retrieveColumn(name: String): Unit = {
    columnRefs.map(r => r.path.toStringWithoutAddress).foreach(s => log.info(s))
    val column = columnRefs.filter(actorRef => actorRef.path.toStringWithoutAddress.endsWith(name)).head
    sender() ! RetrievedColumn(column)
  }

  private def listColumns(): Unit = {
    log.debug("{}", columnRefs.map(actorRef => actorRef.path.toString))
    val columnNames = columnRefs.map(actorRef => actorRef.path.name)
    sender() ! ColumnList(columnNames)
  }

  private def tryToAddRow(rowCount: Int, row: List[String]): Unit = {
    if (rowCount >= partitionSize) {
      log.info("Partition full")
      sender() ! PartitionFull()
    } else if (columnRefs.size != row.size) {
      val future = Future.failed(new Exception("Wrong number of columns"))
      pipe(future) to sender()
    } else {
      addRow(rowCount, row)
    }
  }

  private def addRow(rowCount: Int, row: List[String]): Unit = {
    log.debug("Adding row to partition: {}", row)

    import scala.concurrent.duration._
    implicit val timeout: Timeout = Timeout(5 seconds) // needed for `ask` below

    // TODO: verify that value is appended to correct column
    val listOfFutures = columnRefs.zip(row).map { case (column, value) =>
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
