package de.hpi.svedeb

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.TableManager._
import de.hpi.svedeb.table.{Partition, Table}

object TableManager {
  case class AddTable(name: String, columns: List[String])
  case class RemoveTable(name: String)
  case class ListTables()
  case class FetchTable(name: String)

  case class TableAdded()
  case class TableRemoved()
  case class TableList(tables: List[String])
  case class TableFetched(table: ActorRef)

  def props(): Props = Props(new TableManager())
}

class TableManager extends Actor with ActorLogging {
  override def receive: Receive = active(Map.empty[String, ActorRef])

  private def addTable(tables: Map[String, ActorRef], name: String, columns: List[String]): Unit = {
    log.info("Adding Table")
    val table = context.actorOf(Table.props(columns, 10), "name")
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
    case AddTable(name, columns) => addTable(tables, name, columns)
    case RemoveTable(name) => removeTable(tables, name)
    case ListTables() => sender() ! TableList(tables.keys.toList)
    case FetchTable(name) => fetchTable(tables, name)
  }
}
