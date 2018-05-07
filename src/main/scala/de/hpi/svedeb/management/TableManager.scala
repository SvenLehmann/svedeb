package de.hpi.svedeb.management

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.management.TableManager._
import de.hpi.svedeb.table.Table

object TableManager {
  case class AddTable(name: String, columnNames: Seq[String])
  case class RemoveTable(name: String)
  case class ListTables()
  case class FetchTable(name: String)

  case class TableAdded()
  case class TableRemoved()
  case class TableList(tableNames: Seq[String])
  case class TableFetched(table: ActorRef)

  def props(): Props = Props(new TableManager())
}

class TableManager extends Actor with ActorLogging {
  override def receive: Receive = active(Map.empty[String, ActorRef])

  private def addTable(tables: Map[String, ActorRef], name: String, columnNames: Seq[String]): Unit = {
    log.info("Adding Table")
    val table = context.actorOf(Table.props(columnNames, 10), "name")
    val newTables = tables + (name -> table)
    context.become(active(newTables))
    sender() ! TableAdded()
  }

  def removeTable(tables: Map[String, ActorRef], name: String): Unit = {
    val newTables = tables - name
    context.become(active(newTables))
    sender() ! TableRemoved()
  }

  def fetchTable(tables: Map[String, ActorRef], name: String): Unit = {
    val tableRef = tables.get(name)
    if (tableRef.isDefined) {
      sender() ! TableFetched(tableRef.get)
    } else {
      sender() ! Failure(new Exception("Table not found"))
    }
  }

  private def active(tables: Map[String, ActorRef]): Receive = {
    case AddTable(name, columnNames) => addTable(tables, name, columnNames)
    case RemoveTable(name) => removeTable(tables, name)
    case ListTables() => sender() ! TableList(tables.keys.toList)
    case FetchTable(name) => fetchTable(tables, name)
  }
}
