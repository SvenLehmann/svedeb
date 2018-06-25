package de.hpi.svedeb.management

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import de.hpi.svedeb.management.TableManager._
import de.hpi.svedeb.table.{ColumnType, Table}
import de.hpi.svedeb.utils.Utils

object TableManager {
  case class AddTable(name: String, data: Map[Int, Map[String, ColumnType]], partitionSize: Int = Utils.defaultPartitionSize)
  case class RemoveTable(name: String)
  case class ListTables()
  case class FetchTable(name: String)
  case class AddNewTableManager(newTableManager: ActorRef)
  case class ListRemoteTableManagers()

  case class TableAdded(table: ActorRef)
  case class TableRemoved()
  case class TableList(tableNames: Seq[String])
  case class TableFetched(table: ActorRef)
  case class RemoteTableManagers(tableManagers: Seq[ActorRef])

  def props(remoteTableManagers: Seq[ActorRef] = Seq.empty): Props = Props(new TableManager(remoteTableManagers))

  private case class TableManagerState(tables: Map[String, ActorRef], remoteTableManagers: Seq[ActorRef]) {
    def addTableManager(tableManager: ActorRef) : TableManagerState = {
      TableManagerState(tables, remoteTableManagers :+ tableManager)
    }

    def addTable(tableName: String, table: ActorRef) : TableManagerState = {
      TableManagerState(tables + (tableName -> table), remoteTableManagers)
    }

    def removeTable(tableName: String): TableManagerState = {
      TableManagerState(tables - tableName, remoteTableManagers)
    }
  }
}

class TableManager(remoteTableManagers: Seq[ActorRef]) extends Actor with ActorLogging {
  override def receive: Receive = active(TableManagerState(Map.empty, remoteTableManagers))

  private def addTable(state: TableManagerState,
                       name: String,
                       data: Map[Int, Map[String, ColumnType]],
                       partitionSize: Int): Unit = {
    log.debug("Adding Table")
    val table = context.actorOf(Table.propsWithData(data, partitionSize))
    val newTables = state.addTable(name, table)
    context.become(active(newTables))
    sender() ! TableAdded(table)
  }

  private def storeNewTableManager(state: TableManagerState, tableManager: ActorRef): Unit = {
    context.become(active(state.addTableManager(tableManager)))
  }

  private def removeTable(state: TableManagerState, name: String): Unit = {
    val oldTable = state.tables.getOrElse(name, ActorRef.noSender)

    context.become(active(state.removeTable(name)))

    if (oldTable != ActorRef.noSender) {
      oldTable ! PoisonPill
    }
    sender() ! TableRemoved()
  }

  private def fetchTable(state: TableManagerState, name: String): Unit = {
    val tableRef = state.tables.get(name)
    if (tableRef.isDefined) {
      sender() ! TableFetched(tableRef.get)
    } else {
      sender() ! Failure(new Exception("Table not found"))
    }
  }

  private def active(state: TableManagerState): Receive = {
    case AddNewTableManager(tableManager) => storeNewTableManager(state, tableManager)
    case ListRemoteTableManagers() => sender() ! RemoteTableManagers(state.remoteTableManagers)
    case AddTable(name, data, partitionSize) => addTable(state, name, data, partitionSize)
    case RemoveTable(name) => removeTable(state, name)
    case ListTables() => sender() ! TableList(state.tables.keys.toList)
    case FetchTable(name) => fetchTable(state, name)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
