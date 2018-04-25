package de.hpi.svedeb.table

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import de.hpi.svedeb.table.Column.AppendValue
import de.hpi.svedeb.table.Partition._

object Partition {
  case class GetColumnsMessage()
  case class AddColumnMessage(name: String)
  case class DropColumnMessage(name: String)
  case class AddRowToPartitionMessage(row: List[String])

  // Just for returning values
  case class ColumnListMessage(columns: List[ActorRef])

  def props(): Props = Props(new Partition())
}

class Partition extends Actor {
  val log = Logging(context.system, this)

  override def receive: Receive = active(List.empty[ActorRef])

  def active(columns: List[ActorRef]): Receive = {
    case GetColumnsMessage => sender ! ColumnListMessage(columns)
    case AddColumnMessage(name) => addColumn(columns, name)
    case DropColumnMessage(name) => dropColumn(columns, name)
    case AddRowToPartitionMessage(row) => addRow(columns, row)
    case _ => log.error("Message not understood")
  }

  // TODO: Must be synchronized with all other partition of this table
  def addColumn(columns: List[ActorRef], name: String): Unit = {
    log.debug("Adding new column to partition with name: {}", name)
    val newColumn = context.actorOf(Column.props(name), name)
    context.become(active(newColumn :: columns))
  }

  // TODO: Must be synchronized with all other partition of this table
  def dropColumn(columns: List[ActorRef], name: String): Unit = {
    log.debug("Dropping column from partition with name: {}", name)
    context.become(active(columns.filter(actorRef => actorRef.path.name != name)))
  }

  def addRow(columns: List[ActorRef], row: List[String]): Unit = {
    log.debug("Adding row to partition: {}", row)
    // TODO: verify that value is appended to correct column
    // TODO: size of columns and row must be equal
    for((column, value) <- columns zip row) yield column ! AppendValue(value)
  }
}
