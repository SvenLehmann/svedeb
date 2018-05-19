package de.hpi.svedeb.table

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import de.hpi.svedeb.table.Partition._
import de.hpi.svedeb.table.Table._
import de.hpi.svedeb.utils.Utils

import scala.concurrent.Future

object Table {
  case class AddColumnToTable(name: String)
  case class AddRowToTable(row: RowType)
  case class ListColumnsInTable()
  case class GetColumnFromTable(columnName: String)
  case class GetPartitions()

  // Result events
  case class ColumnAddedToTable()
  case class RowAddedToTable()
  case class ColumnList(columnNames: Seq[String])
  case class ActorsForColumn(columnName: String, columnActors: Seq[ActorRef])
  case class PartitionsInTable(partitions: Seq[ActorRef])

  def props(columnNames: Seq[String], partitionSize: Int = Utils.defaultPartitionSize, initialPartitions: Seq[ActorRef] = Seq.empty[ActorRef]): Props = Props(new Table(columnNames, partitionSize, initialPartitions))
}

class Table(columnNames: Seq[String], partitionSize: Int, initialPartitions: Seq[ActorRef]) extends Actor with ActorLogging {
  import context.dispatcher

  override def receive: Receive = active(initialPartitions)

  private def listColumns(): ColumnList = {
    log.debug("Listing columns: {}", columnNames)
    ColumnList(columnNames)
  }

  private def getColumns(partitions: Seq[ActorRef], columnName: String): Future[ActorsForColumn] = {
    import scala.concurrent.duration._
    implicit val timeout: Timeout = Timeout(5 seconds) // needed for `ask` below

    val listOfFutures = partitions.map(p => ask(p, GetColumn(columnName)).mapTo[ColumnRetrieved])
    Future.sequence(listOfFutures)
      .map(list => list.map(c => c.column))
      .map(c => ActorsForColumn(columnName, c))
  }

  private def handlePartitionFull(partitions: Seq[ActorRef], row: RowType, originalSender: ActorRef): Unit = {
    log.debug("Creating new partition")
    val newPartitionId = partitions.size
    val newPartition = context.actorOf(Partition.props(newPartitionId, columnNames, partitionSize), s"partition$newPartitionId")
    val updatedPartitions = partitions :+ newPartition
    context.become(active(updatedPartitions))
    newPartition ! AddRow(row, originalSender)
  }

  private def handleAddRow(partitions: Seq[ActorRef], row: RowType): Unit = {
    if (partitions.isEmpty) {
      val newPartition = context.actorOf(Partition.props(0, columnNames, partitionSize))
      newPartition ! AddRow(row, sender())

      val newPartitions = partitions :+ newPartition
      context.become(active(newPartitions))
    } else {
      partitions.last ! AddRow(row, sender())
    }
  }

  private def handleGetPartitions(partitions: Seq[ActorRef]): Unit = {
    log.debug("Handling GetPartitions")
    log.debug(s"${sender().path}")
    sender() ! PartitionsInTable(partitions)
  }

  private def active(partitions: Seq[ActorRef]): Receive = {
    case AddRowToTable(row) => handleAddRow(partitions, row)
    case ListColumnsInTable() => sender() ! listColumns()
    case GetColumnFromTable(columnName) => pipe(getColumns(partitions, columnName)) to sender()
    case GetPartitions() => handleGetPartitions(partitions)
    case RowAdded(originalSender) => originalSender ! RowAddedToTable()
    case PartitionFull(row, originalSender) => handlePartitionFull(partitions, row, originalSender)
    case m => throw new Exception(s"Message not understood: $m")
  }
}


