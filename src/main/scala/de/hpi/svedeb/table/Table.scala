package de.hpi.svedeb.table

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import de.hpi.svedeb.table.Partition.{ColumnNameList => _, _}
import de.hpi.svedeb.table.Table._

import scala.concurrent.Future

object Table {

  def props(columnNames: Seq[String], partitionSize: Int, initialPartitions: Seq[ActorRef] = Seq.empty[ActorRef]): Props = Props(new Table(columnNames, partitionSize, initialPartitions))

  case class AddColumnToTable(name: String)
  case class AddRowToTable(row: RowType)
  case class ListColumnsInTable()
  case class GetColumnFromTable(columnName: String)
  case class GetPartitions()

  // Result events
  case class ColumnAddedToTable()
  case class RowAddedToTable()
  case class ColumnList(columnNames: Seq[String])
  case class ActorsForColumn(columnActors: Seq[ActorRef])
  case class PartitionsInTable(partitions: Seq[ActorRef])
}

class Table(columnNames: Seq[String], partitionSize: Int, initialPartitions: Seq[ActorRef]) extends Actor with ActorLogging {
  import context.dispatcher

  // Initialize with single partition
  override def receive: Receive = {
    if (initialPartitions.isEmpty) {
      val newPartition = context.actorOf(Partition.props(columnNames, partitionSize), "partition0")
      active(Seq(newPartition))
    } else {
      active(initialPartitions)
    }
  }

  private def active(partitions: Seq[ActorRef]): Receive = {
    case AddRowToTable(row) => addRow(partitions, row)
    case ListColumnsInTable() => sender() ! listColumns()
    case GetColumnFromTable(columnName) => pipe(getColumns(partitions, columnName)) to sender()
    case GetPartitions() => sender() ! PartitionsInTable(partitions)
    case x => log.error("Message not understood: {}", x)
  }

  private def listColumns(): ColumnList = {
    log.info("Listing columns: {}", columnNames)
    ColumnList(columnNames)
  }

  private def getColumns(partitions: Seq[ActorRef], columnName: String): Future[ActorsForColumn] = {
    import scala.concurrent.duration._
    implicit val timeout: Timeout = Timeout(5 seconds) // needed for `ask` below

    val listOfFutures = partitions.map(p => ask(p, GetColumn(columnName)).mapTo[ColumnRetrieved])
    val foo = Future.sequence(listOfFutures)
    val bar = foo.map(list => list.map(c => c.column)).map(c => ActorsForColumn(c))
    bar
  }

  private def addRow(partitions: Seq[ActorRef], row: RowType): Unit = {
    log.debug("Going to add row to table: {}", row)
    tryToAddRow(sender(), partitions, row)
  }

  private def tryToAddRow(sender: ActorRef, partitions: Seq[ActorRef], row: RowType): Unit = {
    import scala.concurrent.duration._
    implicit val timeout: Timeout = Timeout(5 seconds) // needed for `ask` below

    val response = ask(partitions.last, AddRow(row))
    response.foreach {
      case RowAdded() =>
        log.info("Adding to existing partition")
        sender ! RowAddedToTable()
      case PartitionFull() =>
        log.info("Creating new partition")
        val newPartitionId = partitions.size
        val newPartition = context.actorOf(Partition.props(columnNames, partitionSize), "partition" + newPartitionId)
        val updatedPartitions = partitions :+ newPartition
        context.become(active(updatedPartitions))
        tryToAddRow(sender, updatedPartitions, row)
    }
  }
}


