package de.hpi.svedeb.management

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import de.hpi.svedeb.management.TableManager._
import de.hpi.svedeb.table.{ColumnType, Partition, Table}

object TableManager {
  case class AddTable(name: String, columnNames: Seq[String], partitions: Map[Int, ActorRef])
  case class AddRemoteTable(name: String, table: ActorRef)
  case class AddPartition(partitionId: Int, partitionData: Map[String, ColumnType], partitionSize: Int)
  case class RemoveTable(name: String)
  case class ListTables()
  case class FetchTable(name: String)
  case class AddRemoteTableManager()
  case class ListRemoteTableManagers()

  case class RemoteTableAdded()
  case class TableAdded(table: ActorRef)
  case class TableRemoved()
  case class TableList(tableNames: Seq[String])
  case class TableFetched(table: ActorRef)
  case class RemoteTableManagers(tableManagers: Seq[ActorRef])
  case class PartitionCreated(partitionId: Int, partition: ActorRef)

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
  remoteTableManagers.foreach(_ ! AddRemoteTableManager())

  override def receive: Receive = active(TableManagerState(Map.empty, remoteTableManagers))

  private def addTable(state: TableManagerState,
                       name: String,
                       columnNames: Seq[String],
                       partitions: Map[Int, ActorRef]): Unit = {
    log.debug("Add table")
    val table = context.actorOf(Table.propsWithPartitions(columnNames, partitions))
    sender() ! TableAdded(table)

    val newState = state.addTable(name, table)
    context.become(active(newState))
  }

  private def storeNewTableManager(state: TableManagerState, tableManager: ActorRef): Unit = {
    log.debug("store new table manager")
    context.become(active(state.addTableManager(tableManager)))
  }

  private def removeTable(state: TableManagerState, name: String): Unit = {
    log.debug("remove table")
    val oldTable = state.tables.getOrElse(name, ActorRef.noSender)

    context.become(active(state.removeTable(name)))

    if (oldTable != ActorRef.noSender) {
      oldTable ! PoisonPill
    }
    sender() ! TableRemoved()
  }

  private def fetchTable(state: TableManagerState, name: String): Unit = {
    log.debug("fetch table")
    val tableRef = state.tables.get(name)
    if (tableRef.isDefined) {
      sender() ! TableFetched(tableRef.get)
    } else {
      sender() ! Failure(new Exception("Table not found"))
    }
  }

  private def addPartition(partitionId: Int, partitionData: Map[String, ColumnType], partitionSize: Int): Unit = {
    log.debug("Add partition")
    val newPartition = context.actorOf(Partition.props(partitionId, partitionData, partitionSize))
    sender() ! PartitionCreated(partitionId, newPartition)
  }

  private def addRemoteTable(state: TableManagerState, tableName: String, table: ActorRef): Unit = {
    log.debug(s"Adding remote table $tableName")
    val newState = state.addTable(tableName, table)
    context.become(active(newState))

    sender() ! RemoteTableAdded()
  }

  private def active(state: TableManagerState): Receive = {
    case AddRemoteTable(tableName, table) => addRemoteTable(state, tableName, table)
    case AddRemoteTableManager() => storeNewTableManager(state, sender())
    case ListRemoteTableManagers() => sender() ! RemoteTableManagers(state.remoteTableManagers)
    case AddTable(name, columnNames, partitions) => addTable(state, name, columnNames, partitions)
    case AddPartition(partitionId, partitionData, partitionSize) => addPartition(partitionId, partitionData, partitionSize)
    case RemoveTable(name) => removeTable(state, name)
    case ListTables() => sender() ! TableList(state.tables.keys.toList)
    case FetchTable(name) => fetchTable(state, name)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
