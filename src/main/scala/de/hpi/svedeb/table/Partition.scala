package de.hpi.svedeb.table

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import de.hpi.svedeb.table.Column.AppendValue
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

  def props(columns: List[String] = List.empty[String]): Props = Props(new Partition(columns))
}

class Partition(columnNames: List[String]) extends Actor {
  import context.dispatcher

  private val log = Logging(context.system, this)

  // Columns are initialized at actor creation time and cannot be mutated later on.
  private val columnRefs = columnNames.map(name => context.actorOf(Column.props(name)))

  override def receive: Receive = {
    case ListColumns() => sender() ! listColumns()
    case GetColumn(name) => sender() ! getColumn(name)
    case AddRow(row) => pipe(addRow(row)) to sender()
    case x => log.error("Message not understood: {}", x)
  }

  private def getColumn(name: String): RetrievedColumn = {
    val column = columnRefs.filter(actorRef => actorRef.path.name != name).head
    RetrievedColumn(column)
  }
  private def listColumns(): ColumnList = {
    log.debug("{}", columnRefs.map(actorRef => actorRef.path.toString))
    val columnNames = columnRefs.map(actorRef => actorRef.path.name)
    ColumnList(columnNames)
  }

  private def addRow(row: List[String]): Future[RowAdded] = {
    log.debug("Adding row to partition: {}", row)

    if (columnRefs.size != row.size) {
      Future.failed(new Exception("Wrong number of columns"))
    } else {
      // TODO: verify that value is appended to correct column
      for((column, value) <- columnRefs zip row) yield column ! AppendValue(value)

      import scala.concurrent.duration._
      implicit val timeout: Timeout = Timeout(5 seconds) // needed for `ask` below

      val listOfFutures = columnRefs.zip(row).map{ case (column, value) => ask(column, AppendValue(value))}
      Future.sequence(listOfFutures).map(_ => RowAdded())
    }
  }

}
