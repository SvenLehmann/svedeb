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

  // Initialize with single partition
  override def receive: Receive = {
    if (initialPartitions.isEmpty) {
      val newPartition = context.actorOf(Partition.props(0, columnNames, partitionSize), "partition0")
      active(Seq(newPartition))
    } else {
      active(initialPartitions)
    }
  }

  private def listColumns(): ColumnList = {
    log.info("Listing columns: {}", columnNames)
    ColumnList(columnNames)
  }

  private def getColumns(partitions: Seq[ActorRef], columnName: String): Future[ActorsForColumn] = {
    import scala.concurrent.duration._
    implicit val timeout: Timeout = Timeout(5 seconds) // needed for `ask` below

    val listOfFutures = partitions.map(p => ask(p, GetColumn(columnName)).mapTo[ColumnRetrieved])
    Future.sequence(listOfFutures)
      .map(list => list.map(c => c.column))
      .map(c => ActorsForColumn("Some Column Name", c)) //TODO!
  }

  private def active(partitions: Seq[ActorRef]): Receive = {
    case AddRowToTable(row) => partitions.last ! AddRow(row, sender())
    case ListColumnsInTable() => sender() ! listColumns()
    case GetColumnFromTable(columnName) => pipe(getColumns(partitions, columnName)) to sender()
    case GetPartitions() => sender() ! PartitionsInTable(partitions)
    case RowAdded(originalSender) =>
      log.info("Adding to existing partition")
      originalSender ! RowAddedToTable()
    case PartitionFull(row, originalSender) =>
      log.info("Creating new partition")
      val newPartitionId = partitions.size
      val newPartition = context.actorOf(Partition.props(newPartitionId, columnNames, partitionSize), "partition" + newPartitionId)
      val updatedPartitions = partitions :+ newPartition
      context.become(active(updatedPartitions))
      newPartition ! AddRow(row, originalSender)
    case m => throw new Exception("Message not understood: " + m)
  }
}


