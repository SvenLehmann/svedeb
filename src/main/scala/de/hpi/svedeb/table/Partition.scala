package de.hpi.svedeb.table

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import de.hpi.svedeb.table.Column.AppendValue
import de.hpi.svedeb.table.Partition._

object Partition {
  case class ListColumns()
  case class GetColumn(name: String)
  case class AddColumn(name: String)
  case class DropColumn(name: String)
  case class AddRow(row: List[String])

  // Result events
  case class ColumnList(columns: List[String])
  case class Column(column: ActorRef)
  case class ColumnAdded()
  case class ColumnDropped()
  case class RowAdded()

  def props(initialColumns: List[ActorRef] = List.empty[ActorRef]): Props = Props(new Partition(initialColumns))
}

class Partition(initialColumns: List[ActorRef]) extends Actor {
  val log = Logging(context.system, this)

  override def receive: Receive = active(initialColumns)

  private def active(columns: List[ActorRef]): Receive = {
    case ListColumns => sender() ! listColumns(columns)
    case GetColumn(name) => sender() ! getColumn(columns, name)
    case AddColumn(name) => sender() ! addColumn(columns, name)
    case DropColumn(name) => sender() ! dropColumn(columns, name)
    case AddRow(row) => sender() ! addRow(columns, row)
    case _ => log.error("Message not understood")
  }

  private def getColumn(columns: List[ActorRef], name: String): ActorRef = {
    columns.filter(actorRef => actorRef.path.name != name).head
  }
  private def listColumns(columns: List[ActorRef]): ColumnList = {
    val foo = columns.map(actorRef => actorRef.path.name)
    ColumnList(foo)
  }

  // TODO: Must be synchronized with all other partitions of this table
  private def addColumn(columns: List[ActorRef], name: String): ColumnAdded = {
    log.debug("Adding new column to partition with name: {}", name)
    val newColumn = context.actorOf(Column.props(name), name)
    context.become(active(newColumn :: columns))
    ColumnAdded()
  }

  // TODO: Must be synchronized with all other partitions of this table
  private def dropColumn(columns: List[ActorRef], name: String): ColumnDropped = {
    log.debug("Dropping column from partition with name: {}", name)
    context.become(active(columns.filter(actorRef => actorRef.path.name != name)))
    ColumnDropped()
  }

  private def addRow(columns: List[ActorRef], row: List[String]): RowAdded = {
    log.debug("Adding row to partition: {}", row)
    // TODO: verify that value is appended to correct column
    // TODO: size of columns and row must be equal
    for((column, value) <- columns zip row) yield column ! AppendValue(value)

    // TODO: send RowAdded after all columns confirmed insert
    RowAdded()
  }
}
