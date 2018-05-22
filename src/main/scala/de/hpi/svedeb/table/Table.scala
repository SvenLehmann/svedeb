package de.hpi.svedeb.table

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import de.hpi.svedeb.table.Partition._
import de.hpi.svedeb.table.Table._
import de.hpi.svedeb.table.worker.TableWorker
import de.hpi.svedeb.table.worker.TableWorker.{GetColumnFromTableWorker, InternalActorsForColumn}
import de.hpi.svedeb.utils.Utils

import scala.concurrent.Future

object Table {
  case class AddRowToTable(row: RowType)
  case class ListColumnsInTable()
  case class GetColumnFromTable(columnName: String)
  case class GetPartitions()

  // Result events
  case class RowAddedToTable()
  case class ColumnList(columnNames: Seq[String])
  case class ActorsForColumn(columnName: String, columnActors: Seq[ActorRef])
  case class PartitionsInTable(partitions: Seq[ActorRef])

  def props(columnNames: Seq[String], partitionSize: Int = Utils.defaultPartitionSize, initialPartitions: Seq[ActorRef] = Seq.empty[ActorRef]): Props = Props(new Table(columnNames, partitionSize, initialPartitions))

  private case class TableState(cachedColumns: Map[String, Map[Int, ActorRef]], partitions: Seq[ActorRef]) {
    def addPartition(newPartition: ActorRef): TableState = {
      TableState(cachedColumns, partitions :+ newPartition)
    }

    def addColumn(partitionId: Int, columnName: String, column: ActorRef): TableState = {
      val newInnerMap = cachedColumns(columnName) + (partitionId -> column)
      val newColumns = cachedColumns + (columnName -> newInnerMap)
      TableState(newColumns, partitions)
    }

    def isCacheComplete(columnName: String): Boolean = {
      cachedColumns(columnName).size == partitions.size
    }
  }
}

class Table(columnNames: Seq[String], partitionSize: Int, initialPartitions: Seq[ActorRef]) extends Actor with ActorLogging {
  import context.dispatcher

  override def receive: Receive = active(TableState(Map.empty, initialPartitions))

  private def listColumns(): ColumnList = {
    log.debug("Listing columns: {}", columnNames)
    ColumnList(columnNames)
  }

  private def getColumns(state: TableState, columnName: String): Unit = {
    context.actorOf(TableWorker.props(state.partitions)) ! GetColumnFromTableWorker(sender(), columnName)
  }



  private def handlePartitionFull(state: TableState, row: RowType, originalSender: ActorRef): Unit = {
    log.debug("Creating new partition")
    val newPartitionId = state.partitions.size
    val newPartition = context.actorOf(Partition.props(newPartitionId, columnNames, partitionSize), s"partition$newPartitionId")
    val newState = state.addPartition(newPartition)
    context.become(active(newState))
    newPartition ! AddRow(row, originalSender)
  }

  private def handleAddRow(state: TableState, row: RowType): Unit = {
    if (state.partitions.isEmpty) {
      val newPartition = context.actorOf(Partition.props(0, columnNames, partitionSize))
      newPartition ! AddRow(row, sender())

      val newState = state.addPartition(newPartition)
      context.become(active(newState))
    } else {
      state.partitions.last ! AddRow(row, sender())
    }
  }

  private def handleGetPartitions(state: TableState): Unit = {
    log.debug("Handling GetPartitions")
    log.debug(s"${sender().path}")
    sender() ! PartitionsInTable(state.partitions)
  }

  private def active(state: TableState): Receive = {
    case AddRowToTable(row) => handleAddRow(state, row)
    case ListColumnsInTable() => sender() ! listColumns()
    case GetColumnFromTable(columnName) => getColumns(state, columnName)
    case GetPartitions() => handleGetPartitions(state)
    case RowAdded(originalSender) => originalSender ! RowAddedToTable()
    case PartitionFull(row, originalSender) => handlePartitionFull(state, row, originalSender)
    case InternalActorsForColumn(originalSender, columnName, columnActors) => originalSender ! ActorsForColumn(columnName, columnActors)
    case m => throw new Exception(s"Message not understood: $m")
  }
}


