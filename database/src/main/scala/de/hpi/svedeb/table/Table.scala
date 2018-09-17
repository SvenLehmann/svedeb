package de.hpi.svedeb.table

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import de.hpi.svedeb.table.Partition._
import de.hpi.svedeb.table.Table._
import de.hpi.svedeb.table.worker.TableWorker
import de.hpi.svedeb.table.worker.TableWorker.{GetColumnFromTableWorker, InternalActorsForColumn}
import de.hpi.svedeb.utils.Utils

object Table {
  case class AddRowToTable(row: RowType)
  case class ListColumnsInTable()
  case class GetColumnFromTable(columnName: String)
  case class GetPartitions()

  // Result events
  case class RowAddedToTable()
  case class ColumnList(columnNames: Seq[String])
  case class ActorsForColumn(columnName: String, columnActors: Map[Int, ActorRef])
  case class PartitionsInTable(partitions: Map[Int, ActorRef])


  def propsWithData(data: Map[Int, Map[String, ColumnType]],
                    partitionSize: Int = Utils.defaultPartitionSize
           ): Props = {
    Props(new TableWithData(data, partitionSize))
  }

  def propsWithPartitions(columnNames: Seq[String],
                          partitions: Map[Int, ActorRef],
                          partitionSize: Int = Utils.defaultPartitionSize): Props = {
    Props(new TableWithPartitions(columnNames, partitions, partitionSize))
  }

  case class TableState(columnNames: Seq[String], partitions: Map[Int, ActorRef]) {
    def addPartition(partitionId: Int, newPartition: ActorRef): TableState = {
      TableState(columnNames, partitions + (partitionId -> newPartition))
    }
  }
}

abstract class Table(partitionSize: Int) extends Actor with ActorLogging {
  private def listColumns(state: TableState): ColumnList = {
    val columnNames = state.columnNames
    log.debug("Listing columns: {}", columnNames)
    ColumnList(columnNames)
  }

  private def fetchColumns(state: TableState, columnName: String): Unit = {
    log.debug("Fetch columns")
    context.actorOf(TableWorker.props(state.partitions)) ! GetColumnFromTableWorker(sender(), columnName)
  }

  private def handlePartitionFull(state: TableState, row: RowType, originalSender: ActorRef): Unit = {
    log.debug("Creating new partition")
    val newPartitionId = state.partitions.size
    val newPartition = createNewPartition(state, newPartitionId)
    val newState = state.addPartition(newPartitionId, newPartition)
    context.become(active(newState))
    newPartition ! AddRow(row, originalSender)
  }

  private def createNewPartition(state: TableState, partitionId: Int): ActorRef = {
    context.actorOf(Partition.props(partitionId, state.columnNames.map(name => (name, ColumnType())).toMap, partitionSize))
  }

  private def handleAddRow(state: TableState, row: RowType): Unit = {
    if (state.partitions.isEmpty) {
      val newPartition = createNewPartition(state, 0)
      newPartition ! AddRow(row, sender())

      val newState = state.addPartition(0, newPartition)
      context.become(active(newState))
    } else {
      state.partitions.last._2 ! AddRow(row, sender())
    }
  }

  private def handleGetPartitions(state: TableState): Unit = {
    log.debug("Handling GetPartitions")
    sender() ! PartitionsInTable(state.partitions)
  }

  protected def active(state: TableState): Receive = {
    case AddRowToTable(row) => handleAddRow(state, row)
    case ListColumnsInTable() => sender() ! listColumns(state)
    case GetColumnFromTable(columnName) => fetchColumns(state, columnName)
    case GetPartitions() => handleGetPartitions(state)
    case RowAdded(originalSender) => originalSender ! RowAddedToTable()
    case PartitionFull(row, originalSender) => handlePartitionFull(state, row, originalSender)
    case InternalActorsForColumn(originalSender, columnName, columnActors) =>
      originalSender ! ActorsForColumn(columnName, columnActors)
    case PoisonPill => state.partitions.values.foreach(_ ! PoisonPill)
    case m => throw new Exception(s"Message not understood: $m")
  }
}

class TableWithData(data: Map[Int, Map[String, ColumnType]], partitionSize: Int) extends Table(partitionSize) {
  override def receive: Receive = active(TableState(
    data.headOption.map { case (_, partition) => partition.keys.toSeq}.getOrElse(Seq()),
    data.map { case (id, partitionData) => (id, context.actorOf(Partition.props(id, partitionData, partitionSize))) }
  ))
}

class TableWithPartitions(columnNames: Seq[String], partitions: Map[Int, ActorRef], partitionSize: Int) extends Table(partitionSize) {
  override def receive: Receive = active(TableState(
    columnNames,
    partitions
  ))
}


