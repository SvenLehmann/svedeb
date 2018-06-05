package de.hpi.svedeb.management

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.management.TableManager._
import de.hpi.svedeb.table.Table
import de.hpi.svedeb.utils.Utils

object TableManager {
  case class AddTable(name: String, columnNames: Seq[String], partitionSize: Int = Utils.defaultPartitionSize)
  case class RemoveTable(name: String)
  case class ListTables()
  case class FetchTable(name: String)

  case class TableAdded(table: ActorRef)
  case class TableRemoved()
  case class TableList(tableNames: Seq[String])
  case class TableFetched(table: ActorRef)

  def props(): Props = Props(new TableManager())
}

class TableManager extends Actor with ActorLogging {
  override def receive: Receive = active(Map.empty[String, ActorRef])

  private def addTable(tables: Map[String, ActorRef],
                       name: String, columnNames: Seq[String],
                       partitionSize: Int): Unit = {
    log.debug("Adding Table")
    val table = context.actorOf(Table.props(columnNames, partitionSize), name)
    val newTables = tables + (name -> table)
    context.become(active(newTables))
    sender() ! TableAdded(table)
  }

  private def removeTable(tables: Map[String, ActorRef], name: String): Unit = {
    val newTables = tables - name
    context.become(active(newTables))
    sender() ! TableRemoved()
  }

  private def fetchTable(tables: Map[String, ActorRef], name: String): Unit = {
    val tableRef = tables.get(name)
    if (tableRef.isDefined) {
      sender() ! TableFetched(tableRef.get)
    } else {
      sender() ! Failure(new Exception("Table not found"))
    }
  }

  private def active(tables: Map[String, ActorRef]): Receive = {
    case AddTable(name, columnNames, partitionSize) => addTable(tables, name, columnNames, partitionSize)
    case RemoveTable(name) => removeTable(tables, name)
    case ListTables() => sender() ! TableList(tables.keys.toList)
    case FetchTable(name) => fetchTable(tables, name)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
